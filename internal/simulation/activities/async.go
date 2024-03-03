package activities

import (
	"AsyncDB/pkg/util"
	"math/rand"
	"sync"
)

type AsyncSimulator struct {
	config *Config
}

func NewAsyncSimulator(config *Config) *AsyncSimulator {
	return &AsyncSimulator{
		config: config,
	}
}

func (s *AsyncSimulator) ValidateCheckout() {
}

func (s *AsyncSimulator) ValidateAvailability() {
	orderItemsCnt := s.config.OrderItemsCnt
	wg := &sync.WaitGroup{}
	wg.Add(orderItemsCnt)
	for range orderItemsCnt {
		go func() {
			defer wg.Done()
			util.SimulateSyncIoLoad()
			util.SimulateCpuLoad(100)
		}()
	}
	wg.Wait()
	skuItemsCnt := s.config.SKUItemsCnt
	wg.Add(skuItemsCnt)
	for range skuItemsCnt {
		go func() {
			defer wg.Done()
			util.SimulateSyncIoLoad()
			util.SimulateCpuLoad(100)
		}()
	}
	wg.Wait()
	util.SimulateCpuLoad(10000)
}

func (s *AsyncSimulator) VerifyCustomer() {
	wg := &sync.WaitGroup{}
	util.SimulateSyncIoLoad()
	util.SimulateCpuLoad(100)
	appliedOffersCnt := s.config.AppliedOffersCnt
	wg.Add(appliedOffersCnt)
	for range appliedOffersCnt {
		go func() {
			defer wg.Done()
			isLimitedUse := rand.Intn(2) == 0
			if isLimitedUse {
				util.SimulateSyncIoLoad()
				util.SimulateCpuLoad(1000)
			}
		}()
	}
	wg.Wait()
}

func (s *AsyncSimulator) ValidatePayment() {
	util.SimulateSyncIoLoad()
	paymentsCnt := s.config.PaymentsCnt
	wg := &sync.WaitGroup{}
	wg.Add(paymentsCnt)
	for range paymentsCnt {
		go func() {
			defer wg.Done()
			isActive := rand.Intn(10) < 4
			if isActive {
				util.SimulateSyncIoLoad()
				util.SimulateCpuLoad(10000)
				util.SimulateSyncIoLoad()
				util.SimulateSyncIoLoad()
			}
		}()
	}
	wg.Wait()
}

func (s *AsyncSimulator) ValidateProductOption() {

}

func (s *AsyncSimulator) RecordOffer() {
	util.SimulateSyncIoLoad()
	util.SimulateCpuLoad(10000)
}

func (s *AsyncSimulator) CommitTax() {
	util.SimulateSyncIoLoad()
	util.SimulateSyncIoLoad()
}

func (s *AsyncSimulator) DecrementInventory() {
	util.SimulateSyncIoLoad()
	orderItemsCnt := s.config.OrderItemsCnt
	wg := &sync.WaitGroup{}
	wg.Add(orderItemsCnt)
	for range orderItemsCnt {
		go func() {
			defer wg.Done()
			util.SimulateSyncIoLoad()
			util.SimulateCpuLoad(1000)
			util.SimulateSyncIoLoad()
		}()
	}
	wg.Wait()
}

func (s *AsyncSimulator) CompleteOrder() {
	util.SimulateSyncIoLoad()
}
