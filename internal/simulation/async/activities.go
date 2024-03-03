package asyncwf

import (
	"AsyncDB/internal/simulation"
	"AsyncDB/pkg/util"
	"math/rand"
	"sync"
)

func validateCheckout(config *simulation.Config) {
}

func validateAvailability(config *simulation.Config) {
	orderItemsCnt := config.OrderItemsCnt
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
	skuItemsCnt := config.SKUItemsCnt
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

func verifyCustomer(config *simulation.Config) {
	wg := &sync.WaitGroup{}
	util.SimulateSyncIoLoad()
	util.SimulateCpuLoad(100)
	appliedOffersCnt := config.AppliedOffersCnt
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

func validatePayment(config *simulation.Config) {
	util.SimulateSyncIoLoad()
	paymentsCnt := config.PaymentsCnt
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

func recordOffer(config *simulation.Config) {
	util.SimulateSyncIoLoad()
	util.SimulateCpuLoad(10000)
}

func commitTax(config *simulation.Config) {
	util.SimulateSyncIoLoad()
	util.SimulateSyncIoLoad()
}

func decrementInventory(config *simulation.Config) {
	util.SimulateSyncIoLoad()
	orderItemsCnt := config.OrderItemsCnt
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

func completeOrder(config *simulation.Config) {
	util.SimulateSyncIoLoad()
}
