package asyncwf

import (
	"AsyncDB/pkg/util"
	"math/rand"
	"sync"
)

func validateCheckout() {
}

func validateAvailability() {
	orderItemsCnt := rand.Intn(20) + 1 // Item count range: [1, 20]
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
	skuItemsCnt := rand.Intn(orderItemsCnt) + 1 // SKU count range: [1, orderItemsCnt]
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

func verifyCustomer() {
	wg := &sync.WaitGroup{}
	util.SimulateSyncIoLoad()
	util.SimulateCpuLoad(100)
	appliedOffersCnt := rand.Intn(10) + 1 // Offer count range: [1, 10]
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

func validatePayment() {
	util.SimulateSyncIoLoad()
	paymentsCnt := rand.Intn(7) + 1
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

func recordOffer() {
	util.SimulateSyncIoLoad()
	util.SimulateCpuLoad(10000)
}

func commitTax() {
	util.SimulateSyncIoLoad()
	util.SimulateSyncIoLoad()
}

func decrementInventory() {
	util.SimulateSyncIoLoad()
	orderItemsCnt := rand.Intn(20) + 1 // Item count range: [1, 20]
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

func completeOrder() {
	util.SimulateSyncIoLoad()
}
