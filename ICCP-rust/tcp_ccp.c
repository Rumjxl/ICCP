#include "tcp_ccp.h"
#include "libccp/ccp.h"
#include "libccp/ccp_error.h"

#if (__KERNEL_VERSION_MAJOR__ > 5) || \
    (__KERNEL_VERSION_MAJOR__ == 5 && __KERNEL_VERSION_MINOR__ >= 18)
#define RATESAMPLE_MODE
#elif __KERNEL_VERSION_MAJOR__ == 4 && __KERNEL_VERSION_MINOR__ >= 19
#define RATESAMPLE_MODE
#endif

#define IPC_NETLINK 0
#define IPC_CHARDEV 1

#if __IPC__ == IPC_NETLINK
#include "ccp_nl.h"
#elif __IPC__ == IPC_CHARDEV
#include "ccpkp/ccpkp.h"
#endif

#include <linux/atomic.h>
#include <linux/compiler.h>

#include <linux/module.h>
#include <linux/time64.h>
#include <linux/timekeeping.h>
#include <net/tcp.h>

#define CCP_FRAC_DENOM 10
#define CCP_EWMA_RECENCY 6

static int ccp_kernel_log_level = WARN;
module_param_named(log_level, ccp_kernel_log_level, int, 0644);
MODULE_PARM_DESC(log_level, "Minimum libccp log level: 0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR");

// Global internal state -- allocated during ccp_init and freed in ccp_free.
struct ccp_datapath *kernel_datapath;

void ccp_set_pacing_rate(struct sock *sk, uint32_t rate_abs) {
    // 
    // struct tcp_sock *tp;
    // tp = tcp_sk(sk);
    // u64 rate;
    // rate = tcp_mss_to_mtu(sk, tp->mss_cache);
    // rate *= S_TO_US;
    // rate *= max(tp->snd_cwnd, tp->packets_out);
    // if (likely(tp->srtt_us>>3))
	// 	do_div(rate, tp->srtt_us>>3);
    // WRITE_ONCE((sk->sk_pacing_rate),min_t(u64, rate,sk->sk_max_pacing_rate));
    // if (rate_abs > rate)
    //     pr_debug("[ccp-jxl] pacing rate in initial ccp is larger than in deepcc controled\n");
    sk->sk_pacing_rate = rate_abs;
}

static int rate_sample_valid(const struct rate_sample *rs) {
  int ret = 0;
  if (rs->delivered <= 0)
    ret |= 1;
  if (rs->interval_us <= 0)
    ret |= 1 << 1;
  if (rs->rtt_us <= 0)
    ret |= 1 << 2;
  return ret;
}

static inline void get_sock_from_ccp(
    struct sock **sk,
    struct ccp_connection *conn
) {
    *sk = (struct sock*) ccp_get_impl(conn);
}

static void do_set_cwnd(
    struct ccp_connection *conn, 
    uint32_t cwnd
) {
    struct sock *sk;
    struct tcp_sock *tp;
    get_sock_from_ccp(&sk, conn);
    tp = tcp_sk(sk);

    // translate cwnd value back into packets
    cwnd /= tp->mss_cache;
    tp->snd_cwnd = cwnd;
}

static void do_set_rate_abs(
    struct ccp_connection *conn, 
    uint32_t rate
) {
    struct sock *sk;
    get_sock_from_ccp(&sk, conn);
    ccp_set_pacing_rate(sk, rate);
}

static u64 ccp_now(void) {
    struct timespec64 now;
    ktime_get_real_ts64(&now);
    return timespec64_to_ns(&now);
}

static u64 ccp_since(u64 then) {
    u64 now = ccp_now();
    if (now <= then) {
        return 0;
    }
    return (now - then) / NSEC_PER_USEC;
}

static u64 ccp_after(u64 us) {
    return ccp_now() + us * NSEC_PER_USEC;
}

void ccp_datapath_report_sent(struct ccp_connection *conn) {
    struct sock *sk;
    struct ccp *ca;

    if (!conn) {
        return;
    }

    get_sock_from_ccp(&sk, conn);
    if (!sk) {
        return;
    }

    ca = inet_csk_ca(sk);
    ca->total_reports++;
    ca->invokes_since_last_report = 0;
}

// in dctcp code, in ack event used for ecn information per packet
void tcp_ccp_in_ack_event(struct sock *sk, u32 flags) {
    // according to tcp_input, in_ack_event is called before cong_control, so mmt.ack has old ack value
    const struct tcp_sock *tp = tcp_sk(sk);
    struct ccp *ca = inet_csk_ca(sk);
    struct ccp_primitives *mmt;
    u32 acked_bytes;

#ifdef COMPAT_MODE
    int i=0;
    u64 last_snd_time = 0;
    struct sk_buff *skb = tcp_write_queue_head(sk);
    struct tcp_skb_cb *scb;
#endif

    // if (ca->conn == NULL) {
    //     pr_info("[ccp] ccp_connection not initialized");
    //     return;
    // }
    if (unlikely(!ca || !ca->conn)) {
        pr_warn_ratelimited("[ccp] ca or conn is NULL\n");
        return;
    }
    // pr_info("[ccp-jxl]: conn@%px, conn_id%u", ca->conn, ca->conn->index);
    mmt = &ca->conn->prims;
#ifdef COMPAT_MODE
    for (i=0; i < MAX_SKB_STORED; i++) {
        ca->skb_array[i].first_tx_mstamp = 0;
        ca->skb_array[i].interval_us = 0;
    }

    for (i=0; i < MAX_SKB_STORED; i++) {
        if (skb) {
            scb = TCP_SKB_CB(skb);
            ca->skb_array[i].first_tx_mstamp = skb->skb_mstamp;
            ca->skb_array[i].interval_us = tcp_stamp_us_delta(skb->skb_mstamp, scb->tx.first_tx_mstamp);
            last_snd_time = tcp_skb_timestamp_us(skb);
            skb = skb->next;
        }
    }
    mmt->last_snd_time = last_snd_time;
    mmt->last_rcv_time = tp->tcp_mstamp;
#endif
// #ifdef RATESAMPLE_MODE
//     struct sk_buff *tail_skb = tcp_write_queue_tail(sk);
//     if (tail_skb) {
//         // 内核版本兼容性处理
//         #if __KERNEL_VERSION_MAJOR__ >= 5
//             last_snd_time = ktime_to_us(tail_skb->tstamp);
//         #elif __KERNEL_VERSION_MAJOR__ >= 4 && __KERNEL_VERSION_MINOR__ >= 18
//             last_snd_time = tcp_skb_timestamp_us(tail_skb);
//         #else
//             last_snd_time = tail_skb->skb_mstamp.stamp_us;
//         #endif
//     }
//     else {
//         // 队列为空时的回退逻辑
//         if (skb_queue_empty(&sk->sk_write_queue)) {
//             pr_info("[ccp] Send queue empty");
//             mmt->last_snd_time = (mmt->last_snd_time != 0) ? mmt->last_snd_time : ktime_get_ns() / NSEC_PER_USEC;
//         } else {
//             pr_warn("[ccp] tcp_write_queue_tail NULL but queue not empty");
//             mmt->last_snd_time = ktime_get_ns() / NSEC_PER_USEC;
//         }
//     }

//     mmt->last_rcv_time = tp->tcp_mstamp;

//     pr_debug("[ccp] Queue traversal: found last_snd_time=%llu (queue_len=%d)",
//             mmt->last_snd_time, skb_queue_len(&sk->sk_write_queue));
// #endif
    #ifdef RATESAMPLE_MODE
        // struct sk_buff *skb;
        // u64 max_timestamp = 0;

        // // 遍历发送队列中的所有skb，找到最大的时间戳
        // skb = tcp_send_head(sk);
        // while (skb) {
        //     u64 ts = 0;

        //     // 内核版本兼容性处理
        //     #if __KERNEL_VERSION_MAJOR__ >= 5
        //         ts = ktime_to_us(skb->tstamp);
        //     #elif __KERNEL_VERSION_MAJOR__ >= 4 && __KERNEL_VERSION_MINOR__ >= 18
        //         ts = tcp_skb_timestamp_us(skb);
        //     #else
        //         ts = skb->skb_mstamp.stamp_us;
        //     #endif

        //     if (ts > max_timestamp) {
        //         max_timestamp = ts;
        //         pr_debug("[ccp] Found newer timestamp: %llu (skb=%px)", ts, skb);
        //     }

        //     skb = tcp_write_queue_next(sk, skb);
        // }

        // if (max_timestamp != 0) {
        //     mmt->last_snd_time = max_timestamp;
        //     pr_info("[ccp] Updated last_snd_time: %llu", mmt->last_snd_time);
        // } else if (!skb_queue_empty(&sk->sk_write_queue)) {
        //     // 队列非空但时间戳全为0，可能是内核未设置时间戳
        //     pr_warn("[ccp] All skb timestamps are zero! Using current time");
        //     mmt->last_snd_time = ktime_get_real_ns() / NSEC_PER_USEC;
        // } else {
        //     mmt->last_snd_time = (mmt->last_snd_time != 0) ? mmt->last_snd_time : ktime_get_real_ns() / NSEC_PER_USEC;
        //     pr_info("[ccp] Queue empty, reuse last_snd_time: %llu", mmt->last_snd_time);
        // }
        mmt->last_snd_time = jiffies_to_usecs(tp->lsndtime);
        mmt->last_rcv_time = jiffies_to_usecs(tp->rcv_tstamp);
    #endif
    acked_bytes = tp->snd_una - ca->last_snd_una;
    ca->last_snd_una = tp->snd_una;
    if (acked_bytes) {
        if (flags & CA_ACK_ECE) {
            mmt->ecn_bytes = (u64)acked_bytes;
            mmt->ecn_packets = (u64)acked_bytes / tp->mss_cache;
        } else {
            mmt->ecn_bytes = 0;
            mmt->ecn_packets = 0;
        }
    }
}
EXPORT_SYMBOL_GPL(tcp_ccp_in_ack_event);

/* load the primitive registers of the rate sample - convert all to u64
 * raw values, not averaged
 */
int load_primitives(struct sock *sk, const struct rate_sample *rs) {
    struct tcp_sock *tp = tcp_sk(sk);
    const struct inet_connection_sock *icsk = inet_csk(sk);
    struct ccp *ca = inet_csk_ca(sk);
    struct ccp_primitives *mmt = &ca->conn->prims;
#ifdef COMPAT_MODE
    int i=0;
#endif

    u64 rin = 0; // send bandwidth in bytes per second
    u64 rout = 0; // recv bandwidth in bytes per second
    u64 ack_us = 0;
    u64 snd_us = 0;
    int measured_valid_rate = rate_sample_valid(rs);
    if ( measured_valid_rate != 0 ) {
        return -1;
    }

#ifdef COMPAT_MODE
    // receive rate
    ack_us = tcp_stamp_us_delta(tp->tcp_mstamp, rs->prior_mstamp);

    // send rate
    for (i=0; i < MAX_SKB_STORED; i++) {
        if (ca->skb_array[i].first_tx_mstamp == tp->first_tx_mstamp) {
            snd_us = ca->skb_array[i].interval_us;
            break;
        }
    }
#endif
#ifdef RATESAMPLE_MODE
    ack_us = rs->rcv_interval_us;
    snd_us = rs->snd_interval_us;
#endif

    if (snd_us != 0) {
        rin = (u64)rs->delivered * MTU * S_TO_US;
        do_div(rin, snd_us);
    }

    if (ack_us != 0) {
        rout = (u64)rs->delivered * MTU * S_TO_US;
        do_div(rout, ack_us);
    }

    /*
     * Some real NIC/rate_sample paths frequently report snd_interval_us=0
     * even when the ACK interval is valid. Keeping the previous outgoing rate
     * then makes min(rate_outgoing, rate_incoming) artificially stale/low in
     * CCP programs. Mirror the valid sample so both legacy rate primitives
     * describe the current ACK-clocked delivery sample.
     */
    if (rin == 0 && rout != 0) {
        rin = rout;
    } else if (rout == 0 && rin != 0) {
        rout = rin;
    }

    mmt->bytes_acked = tp->bytes_acked - ca->last_bytes_acked;
    ca->last_bytes_acked = tp->bytes_acked;

    mmt->packets_misordered = tp->sacked_out - ca->last_sacked_out;
    if (tp->sacked_out < ca->last_sacked_out) {
        mmt->packets_misordered = 0;
    } else {
        mmt->packets_misordered = tp->sacked_out - ca->last_sacked_out;
    }

    ca->last_sacked_out = tp->sacked_out;

    mmt->packets_acked = rs->acked_sacked - mmt->packets_misordered;
    mmt->bytes_misordered = mmt->packets_misordered * tp->mss_cache;
    mmt->lost_pkts_sample = rs->losses; //sage.loss = lost_pkts_sample*mss
    mmt->rtt_sample_us = rs->rtt_us;
    if ( rin != 0 ) {
        mmt->rate_outgoing = rin;
    }

    if ( rout != 0 ) {
        mmt->rate_incoming = rout;
    }

    mmt->bytes_in_flight = tcp_packets_in_flight(tp) * tp->mss_cache; 
    mmt->packets_in_flight = tcp_packets_in_flight(tp);
    if (tp->snd_cwnd <= 0) {
        return -1;
    }

    mmt->snd_cwnd = tp->snd_cwnd * tp->mss_cache;

    if (unlikely(tp->snd_una > tp->write_seq)) {
        mmt->bytes_pending = ((u32) ~0U) - (tp->snd_una - tp->write_seq);
    } else {
        mmt->bytes_pending = (tp->write_seq - tp->snd_una);
    }

    /*jxl:sage_info measurement*/
    mmt->ca_state = icsk->icsk_ca_state;
    mmt->snd_ssthresh = tp->snd_ssthresh;
    mmt->snd_mss = tp->mss_cache;/*datapath.info.mss only in init, load info need update*/
    mmt->ato = jiffies_to_usecs(icsk->icsk_ack.ato);/*ato in us*/
    mmt->rto = jiffies_to_usecs(icsk->icsk_rto);/*rto in us*/

    mmt->rttvar = tp->mdev_us >> 2;
    mmt->min_rtt = tcp_min_rtt(tp);
    // mmt->min_rtt = ca->min_rtt;
    // mmt->avg_rtt = ca->avg_rtt;
    // mmt->cnt = ca->cnt;
    mmt->srtt = tp->srtt_us >> 3;

    u32 prate = READ_ONCE(sk->sk_pacing_rate);
	u64 prate64 = prate != ~0U ? prate : ~0ULL;
    mmt->pacing_rate = prate64;

    u32 rate = READ_ONCE(tp->rate_delivered);
	u32 intv = READ_ONCE(tp->rate_interval_us);
	u64 rate64 = 0;
	if (rate && intv) {
		rate64 = (u64)rate * tp->mss_cache * S_TO_US;
		do_div(rate64, intv);
	}
    mmt->delivery_rate = rate64;
    mmt->delivered = tp->delivered - ca->last_pkts_delivered;
    ca->last_pkts_delivered = tp->delivered;
    // mmt->delivered = rs->delivered;
    // mmt->delivered = tp->delivered;
    mmt->bytes_sent= READ_ONCE(tp->bytes_sent);
    mmt->packets_unacked = tp->packets_out;

    // pr_info("[ccp-jxl] in load primitives: bytes_sent: %u\n", mmt->bytes_sent);
    return 0;
    
}

#if (__KERNEL_VERSION_MAJOR__ > 5) || \
    (__KERNEL_VERSION_MAJOR__ == 5 && __KERNEL_VERSION_MINOR__ >= 18)
void tcp_ccp_cong_control(struct sock *sk, u32 ack_event, int flag, const struct rate_sample *rs) {
#else
void tcp_ccp_cong_control(struct sock *sk, const struct rate_sample *rs) {
#endif
    // aggregate measurement
    // state = fold(state, rs)
    int ok;
    struct ccp *ca = inet_csk_ca(sk);
    struct ccp_connection *conn = ca->conn;
    struct tcp_sock *tp = tcp_sk(sk);

#if __IPC__ == IPC_CHARDEV
        ccpkp_try_read();
#endif

    if (conn != NULL) {
        // load primitive registers
        ok = load_primitives(sk, rs);
        if (ok < 0) {
            return;
        }

        ca->total_invokes++;
        ca->invokes_since_last_report++;

        ok = ccp_invoke(conn);
        if (ok == LIBCCP_FALLBACK_TIMED_OUT) {
          pr_warn_ratelimited("[ccp] libccp fallback timed out, to Cubic-like logic\n");
          // TODO default to cubic?
          u32 acked = rs->acked_sacked;
          u32 mss = tp->mss_cache;

          // Cubic-like AIMD handling
          if (rs->losses > 0) {
              // Multiplicative Decrease
              tp->snd_cwnd = max(tp->snd_cwnd >> 1U, 2U);
              tp->snd_ssthresh = max(tp->snd_cwnd, 2U);
          } else {
              // Additive Increase
              if (tcp_in_slow_start(tp)) {
                  // Slow start: exponential growth
                  tp->snd_cwnd += acked;
              } else {
                  // Congestion avoidance: linear growth
                  tp->snd_cwnd += (acked * mss) / tp->snd_cwnd;
              }
              // Clamp cwnd to maximum allowed
              tp->snd_cwnd = min(tp->snd_cwnd, tp->snd_cwnd_clamp);
          }
        //   // Update pacing rate based on new cwnd
        //   u64 rate = (u64)tp->snd_cwnd * mss * USEC_PER_SEC;
        //   do_div(rate, max(tp->srtt_us >> 3, 1U));
        //   sk->sk_pacing_rate = min_t(u64, rate, sk->sk_max_pacing_rate);
        }

        ca->conn->prims.was_timeout = false;
    } else {
        pr_warn_ratelimited("[ccp] ccp_connection not initialized\n");
    }
}
EXPORT_SYMBOL_GPL(tcp_ccp_cong_control);

/* Slow start threshold is half the congestion window (min 2) */
u32 tcp_ccp_ssthresh(struct sock *sk) {
    const struct tcp_sock *tp = tcp_sk(sk);

    return max(tp->snd_cwnd >> 1U, 2U);
}
EXPORT_SYMBOL_GPL(tcp_ccp_ssthresh);

u32 tcp_ccp_undo_cwnd(struct sock *sk) {
    const struct tcp_sock *tp = tcp_sk(sk);

    return max(tp->snd_cwnd, tp->snd_ssthresh << 1);
}
EXPORT_SYMBOL_GPL(tcp_ccp_undo_cwnd);

void tcp_ccp_pkts_acked(struct sock *sk, const struct ack_sample *sample) {
    // struct ccp *cpl;
    // s32 sampleRTT;

    // cpl = inet_csk_ca(sk);
    // sampleRTT = sample->rtt_us;
    // if (rtt_us <= 0) return; 

    // if (ca->min_rtt==0 || rtt_us < ca->min_rtt) {
    //     ca->min_rtt = rtt_us;
    // }
    
    // if (ca->cnt == 0) {
    //     ca->avg_rtt = rtt_us;
    //     ca->cnt++;
    // } else {
    //     u64 tmp_avg = (u64)ca->avg_rtt * ca->cnt + rtt_us;
    //     ca->cnt++;
    //     ca->avg_rtt = tmp_avg / ca->cnt;
    // }
    // pr_info("[ccp-jxl] in sample rtt: min_rtt: %u, avg_rtt: %u, cnt: %u\n", ca->min_rtt, ca->avg_rtt, ca->cnt);
}
EXPORT_SYMBOL_GPL(tcp_ccp_pkts_acked);

/*
 * Detect drops.
 *
 * TCP_CA_Loss -> a timeout happened
 * TCP_CA_Recovery -> an isolated loss (3x dupack) happened.
 * TCP_CA_CWR -> got an ECN
 */
void tcp_ccp_set_state(struct sock *sk, u8 new_state) {
    struct ccp *cpl = inet_csk_ca(sk);
    switch (new_state) {
        case TCP_CA_Loss:
            if (cpl->conn != NULL) {
                cpl->conn->prims.was_timeout = true;
            }
            ccp_invoke(cpl->conn);
            return;
        case TCP_CA_Recovery:
        case TCP_CA_CWR:
        default:
            break;
    }
            
    if (cpl->conn != NULL) {
        cpl->conn->prims.was_timeout = false;
    }
}
EXPORT_SYMBOL_GPL(tcp_ccp_set_state);

void tcp_ccp_init(struct sock *sk) {
    struct ccp *cpl;
    struct tcp_sock *tp = tcp_sk(sk);
    struct ccp_datapath_info dp_info = {
        .init_cwnd = tp->snd_cwnd * tp->mss_cache,
        .mss = tp->mss_cache,
        .src_ip = tp->inet_conn.icsk_inet.inet_saddr,
        .src_port = tp->inet_conn.icsk_inet.inet_sport,
        .dst_ip = tp->inet_conn.icsk_inet.inet_daddr,
        .dst_port = tp->inet_conn.icsk_inet.inet_dport,
        .congAlg = "reno",
    };
    //jxl: *datapath info(init_cwnd:snd_cwnd*mss,per byte), set conAlg
    pr_debug("[ccp] new flow\n");
    
    cpl = inet_csk_ca(sk);
    cpl->last_snd_una = tp->snd_una;
    cpl->last_bytes_acked = tp->bytes_acked;
    cpl->last_sacked_out = tp->sacked_out;
    cpl->last_pkts_delivered = tp->delivered;
    cpl->last_invoke_ns = 0;
    cpl->total_invokes = 0;
    cpl->total_reports = 0;
    cpl->invokes_since_last_report = 0;

    cpl->skb_array = (struct skb_info*)kmalloc(MAX_SKB_STORED * sizeof(struct skb_info), GFP_KERNEL);
    if (!(cpl->skb_array)) {
        pr_err("[ccp] could not allocate skb array\n");
    }
    memset(cpl->skb_array, 0, MAX_SKB_STORED * sizeof(struct skb_info));

    cpl->conn = ccp_connection_start(kernel_datapath, (void *) sk, &dp_info);
    if (cpl->conn == NULL) {
        pr_err("[ccp] start connection failed\n");
    } else {
        pr_debug("[ccp] starting connection %d\n", cpl->conn->index);
    }

    // if no ecn support
    if (!(tp->ecn_flags & TCP_ECN_OK)) {
        INET_ECN_dontxmit(sk);
    }
    
    cmpxchg(&sk->sk_pacing_status, SK_PACING_NONE, SK_PACING_NEEDED);
}
EXPORT_SYMBOL_GPL(tcp_ccp_init);

void tcp_ccp_release(struct sock *sk) {
    struct ccp *cpl = inet_csk_ca(sk);
    if (cpl->conn != NULL) {
        pr_debug("[ccp] freeing connection %d\n", cpl->conn->index);
        ccp_connection_free(kernel_datapath, cpl->conn->index);
        cpl->conn = NULL;
    } else {
        pr_debug("[ccp] already freed\n");
    }
    if (cpl->skb_array != NULL) {
        kfree(cpl->skb_array);
        cpl->skb_array = NULL;
    }
}
EXPORT_SYMBOL_GPL(tcp_ccp_release);

struct tcp_congestion_ops tcp_ccp_congestion_ops = {
    .flags = TCP_CONG_NEEDS_ECN,
    .in_ack_event = tcp_ccp_in_ack_event,
    .name = "ccp",
    .owner = THIS_MODULE,
    .init = tcp_ccp_init,
    .release = tcp_ccp_release,
    .ssthresh = tcp_ccp_ssthresh,
    //.cong_avoid = tcp_ccp_cong_avoid,
    .cong_control = tcp_ccp_cong_control,
    .undo_cwnd = tcp_ccp_undo_cwnd,
    .set_state = tcp_ccp_set_state,
    .pkts_acked = tcp_ccp_pkts_acked
};

void ccp_log(struct ccp_datapath *dp, enum ccp_log_level level, const char* msg, int msg_size) {
    if (level < ccp_kernel_log_level) {
        return;
    }

    switch(level) {
    case ERROR:
        pr_err_ratelimited("%s\n", msg);
        break;
    case WARN:
        pr_warn_ratelimited("%s\n", msg);
        break;
    case INFO:
        pr_info_ratelimited("%s\n", msg);
        break;
    case DEBUG:
        pr_debug_ratelimited("%s\n", msg);
        break;
    case TRACE:
        pr_debug_ratelimited("%s\n", msg);
        break;
    default:
        break;
    }
}

static int __init tcp_ccp_register(void) {
    int ok;

#ifdef COMPAT_MODE
    pr_info("[ccp] Compatibility mode: 4.13 <= kernel version <= 4.16\n");
#endif
#ifdef RATESAMPLE_MODE
    pr_info("[ccp] Rate-sample mode: 4.19 <= kernel version\n");
#endif

    kernel_datapath = kmalloc(sizeof(struct ccp_datapath), GFP_KERNEL);
    if(!kernel_datapath) {
        pr_err("[ccp] could not allocate ccp_datapath\n");
        return -4;
    }

    kernel_datapath->max_connections = MAX_ACTIVE_FLOWS;
    // initializes ccp_active_connections to zeros to support the availability check using index == 0 in ccp_connection_start()
    kernel_datapath->ccp_active_connections =
        (struct ccp_connection *) kzalloc(sizeof(struct ccp_connection) * MAX_ACTIVE_FLOWS, GFP_KERNEL);
    if(!kernel_datapath->ccp_active_connections) {
        pr_err("[ccp] could not allocate ccp_active_connections\n");
        return -5;
    }

    kernel_datapath->max_programs = MAX_DATAPATH_PROGRAMS;
    kernel_datapath->set_cwnd = &do_set_cwnd;
    kernel_datapath->set_rate_abs = &do_set_rate_abs;
    kernel_datapath->now = &ccp_now;
    kernel_datapath->since_usecs = &ccp_since;
    kernel_datapath->after_usecs = &ccp_after;
    kernel_datapath->log = &ccp_log;
    kernel_datapath->fto_us = 1000;
#if __IPC__ == IPC_NETLINK
    ok = ccp_nl_sk(&ccp_read_msg);
    if (ok < 0) {
        return -1;
    }

    kernel_datapath->send_msg = &nl_sendmsg;
    pr_info("[ccp] ipc = netlink\n");
#elif __IPC__ == IPC_CHARDEV
    ok = ccpkp_init(&ccp_read_msg);
    if (ok < 0) {
        return -2;
    }

    kernel_datapath->send_msg = &ccpkp_sendmsg;
    pr_info("[ccp] ipc = chardev\n");
#else
    pr_info("[ccp] ipc =  %s unknown\n", __IPC__);
    return -3;
#endif
	
    ok = ccp_init(kernel_datapath, 0);
    if (ok < 0) {
        pr_err("[ccp] ccp_init failed: %d\n", ok);
#if __IPC__ == IPC_NETLINK
        free_ccp_nl_sk();
#elif __IPC__ == IPC_CHARDEV
        ccpkp_cleanup();
#endif
        return -6;
    }

    pr_info("[ccp] init\n");
    return tcp_register_congestion_control(&tcp_ccp_congestion_ops);
}

static void __exit tcp_ccp_unregister(void) {
    tcp_unregister_congestion_control(&tcp_ccp_congestion_ops);
#if __IPC__ == IPC_NETLINK
    free_ccp_nl_sk();
#elif __IPC__ == IPC_CHARDEV
    ccpkp_cleanup();
#endif
    kfree(kernel_datapath->ccp_active_connections);
    kfree(kernel_datapath);
    pr_info("[ccp] exit\n");
}

module_init(tcp_ccp_register);
module_exit(tcp_ccp_unregister);

MODULE_AUTHOR("Akshay Narayan <akshayn@mit.edu>");
MODULE_DESCRIPTION("Kernel datapath for a congestion control plane");
MODULE_LICENSE("GPL");
