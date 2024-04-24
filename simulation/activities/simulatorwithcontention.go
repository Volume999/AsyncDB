package activities

import "math/rand"

type SimulatorWithContention struct {
	simulator Simulator
	lockCnt   int
	locks     []chan struct{}
}

func NewSimulatorWithContention(simulator Simulator, lockCount int) *SimulatorWithContention {
	locks := make([]chan struct{}, lockCount)
	for i := range locks {
		locks[i] = make(chan struct{}, 1)
	}
	return &SimulatorWithContention{
		simulator: simulator,
		lockCnt:   lockCount,
		locks:     locks,
	}
}

func (s *SimulatorWithContention) acquireLock(lockIndex int) {
	s.locks[lockIndex] <- struct{}{}
}

func (s *SimulatorWithContention) releaseLock(lockIndex int) {
	<-s.locks[lockIndex]
}

func (s *SimulatorWithContention) withLock(f func()) {
	lockIndex := rand.Intn(s.lockCnt)
	s.acquireLock(lockIndex)
	defer s.releaseLock(lockIndex)
	f()
}

func (s *SimulatorWithContention) ValidateCheckout() {
	s.withLock(s.simulator.ValidateCheckout)
}

func (s *SimulatorWithContention) ValidateAvailability() {
	s.withLock(s.simulator.ValidateAvailability)
}

func (s *SimulatorWithContention) VerifyCustomer() {
	s.withLock(s.simulator.VerifyCustomer)
}

func (s *SimulatorWithContention) ValidatePayment() {
	s.withLock(s.simulator.ValidatePayment)
}

func (s *SimulatorWithContention) ValidateProductOption() {
	s.withLock(s.simulator.ValidateProductOption)
}

func (s *SimulatorWithContention) RecordOffer() {
	s.withLock(s.simulator.RecordOffer)
}

func (s *SimulatorWithContention) CommitTax() {
	s.withLock(s.simulator.CommitTax)
}

func (s *SimulatorWithContention) DecrementInventory() {
	s.withLock(s.simulator.DecrementInventory)
}

func (s *SimulatorWithContention) CompleteOrder() {
	s.withLock(s.simulator.CompleteOrder)
}
