//go:build linux
// +build linux

// Copyright 2024 ihciah. All Rights Reserved.

package mem_ring

import (
	"runtime"
	"syscall"
	"unsafe"
)

type Notifier struct {
	fd int32
}

func NewNotifier(fd int32) Notifier {
	return Notifier{fd: fd}
}

func (n Notifier) Notify() {
	val := uint64(1)
	buf := (*[8]byte)(unsafe.Pointer(&val))
	for {
		_, err := syscall.Write(int(n.fd), buf[:])
		if err == syscall.EINTR || err == syscall.EAGAIN {
			runtime.Gosched()
			continue
		}
		return
	}
}

type Awaiter struct {
	fd int32
}

func NewAwaiter(fd int32) Awaiter {
	return Awaiter{fd: fd}
}

func (a *Awaiter) Wait() {
	var val uint64
	buf := (*[8]byte)(unsafe.Pointer(&val))
	for {
		_, err := syscall.Read(int(a.fd), buf[:])
		if err == nil {
			return
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN {
			runtime.Gosched()
			continue
		}
		panic(err)
	}
}
