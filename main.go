package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type relay struct {
	cfg             config
	localIn         *net.UDPConn
	localOut        *net.UDPConn
	localOutputAddr *net.UDPAddr
	network         *net.UDPConn
	peerMu          sync.RWMutex
	peer            *net.UDPAddr
	peerSeen        chan struct{}
}

func main() {
	configPath := flag.String("config", "config.toml", "TOML config path")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	localInAddr, err := net.ResolveUDPAddr("udp", cfg.LocalInputAddr)
	if err != nil {
		log.Fatal(err)
	}
	localOutAddr, err := net.ResolveUDPAddr("udp", cfg.LocalOutputAddr)
	if err != nil {
		log.Fatal(err)
	}
	networkAddr, err := net.ResolveUDPAddr("udp", cfg.NetworkListenAddr)
	if err != nil {
		log.Fatal(err)
	}
	var peer *net.UDPAddr
	if cfg.PeerAddr != "" {
		peer, err = net.ResolveUDPAddr("udp", cfg.PeerAddr)
		if err != nil {
			log.Fatal(err)
		}
	}

	localIn, err := net.ListenUDP("udp", localInAddr)
	if err != nil {
		log.Fatalf("listen local input %s: %v", cfg.LocalInputAddr, err)
	}
	defer localIn.Close()
	// The local output socket remains unconnected: notssh may restart and
	// temporarily close the port without breaking the relay.
	localOut, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Fatalf("open local output %s: %v", cfg.LocalOutputAddr, err)
	}
	defer localOut.Close()
	network, err := net.ListenUDP("udp", networkAddr)
	if err != nil {
		log.Fatalf("listen network %s: %v", cfg.NetworkListenAddr, err)
	}
	defer network.Close()

	r := &relay{cfg: cfg, localIn: localIn, localOut: localOut, localOutputAddr: localOutAddr, network: network, peer: peer, peerSeen: make(chan struct{})}
	if peer != nil {
		close(r.peerSeen)
	}

	log.Printf("[relay] role=%s local=%s -> network=%s -> local=%s rate_limit=%t rate=%d bit/s queue=%d", cfg.Role, cfg.LocalInputAddr, cfg.NetworkListenAddr, cfg.LocalOutputAddr, cfg.RateLimitEnabled, cfg.RateBitsPerSecond, cfg.QueueCapacity)
	if peer != nil {
		log.Printf("[relay] peer=%s", peer.String())
	} else {
		log.Printf("[relay] waiting for the first UDP peer")
	}

	go r.forwardNetworkToLocal(ctx)
	r.forwardLocalToNetwork(ctx)
}

func (r *relay) forwardLocalToNetwork(ctx context.Context) {
	// The FIFO queue preserves multi-packet terminal deltas in full.
	// When full, reading pauses until the oldest packet is sent.
	queue := make(chan []byte, r.cfg.QueueCapacity)
	go func() {
		buf := make([]byte, r.cfg.MaxDatagram)
		for {
			n, _, err := r.localIn.ReadFromUDP(buf)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && ctx.Err() == nil {
					log.Printf("read local input: %v", err)
				}
				close(queue)
				return
			}
			packet := append([]byte(nil), buf[:n]...)
			select {
			case queue <- packet:
			case <-ctx.Done():
				return
			}
		}
	}()

	limiter := newRateLimiter(r.cfg.RateBitsPerSecond)
	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok := <-queue:
			if !ok {
				return
			}
			if !r.waitForPeer(ctx) {
				return
			}
			if r.cfg.RateLimitEnabled {
				if err := limiter.Wait(ctx, len(packet)); err != nil {
					return
				}
			}
			peer := r.currentPeer()
			if peer == nil {
				continue
			}
			if _, err := r.network.WriteToUDP(packet, peer); err != nil && ctx.Err() == nil {
				log.Printf("send network datagram to %s: %v", peer, err)
			}
		}
	}
}

func (r *relay) forwardNetworkToLocal(ctx context.Context) {
	buf := make([]byte, r.cfg.MaxDatagram)
	for {
		n, peer, err := r.network.ReadFromUDP(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && ctx.Err() == nil {
				log.Printf("read network datagram: %v", err)
			}
			return
		}
		if !r.acceptPeer(peer) {
			continue
		}
		if _, err := r.localOut.WriteToUDP(buf[:n], r.localOutputAddr); err != nil && ctx.Err() == nil {
			log.Printf("send local datagram to %s: %v", r.cfg.LocalOutputAddr, err)
		}
	}
}

func (r *relay) acceptPeer(peer *net.UDPAddr) bool {
	r.peerMu.Lock()
	defer r.peerMu.Unlock()
	if r.peer == nil {
		r.peer = cloneUDPAddr(peer)
		select {
		case <-r.peerSeen:
		default:
			close(r.peerSeen)
		}
		log.Printf("[relay] peer learned: %s", peer)
		return true
	}
	return r.peer.IP.Equal(peer.IP) && r.peer.Port == peer.Port
}

func (r *relay) currentPeer() *net.UDPAddr {
	r.peerMu.RLock()
	defer r.peerMu.RUnlock()
	return cloneUDPAddr(r.peer)
}

func (r *relay) waitForPeer(ctx context.Context) bool {
	select {
	case <-r.peerSeen:
		return true
	case <-ctx.Done():
		return false
	}
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	copy := *addr
	copy.IP = append(net.IP(nil), addr.IP...)
	return &copy
}

type rateLimiter struct {
	bitsPerSecond int
	next          time.Time
}

func newRateLimiter(bitsPerSecond int) *rateLimiter {
	return &rateLimiter{bitsPerSecond: bitsPerSecond}
}

func (limiter *rateLimiter) Wait(ctx context.Context, bytes int) error {
	// Nanosecond arithmetic is sufficiently precise for small datagrams;
	// the first packet is sent immediately.
	interval := time.Duration(float64(bytes*8) / float64(limiter.bitsPerSecond) * float64(time.Second))
	now := time.Now()
	if limiter.next.Before(now) {
		limiter.next = now
	}
	deadline := limiter.next
	limiter.next = limiter.next.Add(interval)
	if wait := time.Until(deadline); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
