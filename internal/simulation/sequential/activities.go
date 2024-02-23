package sequentialwf

import (
	"AsyncDB/pkg/util"
	"math/rand"
)

func ValidateCheckout() {
	// This function was not implemented in the original BroadLeaf use-case
}

func ValidateAvailability() {
	orderItemsCnt := rand.Intn(20) + 1 // Item count range: [1, 20]
	for range orderItemsCnt {
		util.SimulateSyncIoLoad() // Load to get the item availability
		util.SimulateCpuLoad(100) // Merge SKU Items
	}
	skuItemsCnt := rand.Intn(orderItemsCnt) + 1 // SKU count range: [1, orderItemsCnt]
	for range skuItemsCnt {
		util.SimulateSyncIoLoad() // Load to get the SKU availability
		util.SimulateCpuLoad(100) // Some operations on SKU Items
	}
}

func VerifyCustomer() {
	util.SimulateSyncIoLoad() // Load to get the customer details
	util.SimulateCpuLoad(100)
	appliedOffersCnt := rand.Intn(10) + 1 // Offer count range: [1, 10]
	for range appliedOffersCnt {
		isLimitedUse := rand.Intn(2) == 0
		if isLimitedUse {
			util.SimulateSyncIoLoad() // Get uses by customer
			util.SimulateCpuLoad(1000)
		}
	}
}

func ValidatePayment() {
	util.SimulateSyncIoLoad() // Get Order
	paymentsCnt := rand.Intn(7) + 1
	for range paymentsCnt {
		isActive := rand.Intn(10) < 4
		if isActive {
			util.SimulateSyncIoLoad() // Make new transaction
			util.SimulateCpuLoad(10000)
			util.SimulateSyncIoLoad()
			util.SimulateSyncIoLoad()
		}
	}
}

func RecordOffer() {
	util.SimulateSyncIoLoad() // Get Order
	util.SimulateCpuLoad(10000)
}

func CommitTax() {
	util.SimulateSyncIoLoad() // Get Order
	util.SimulateSyncIoLoad()
}

func DecrementInventory() {
	util.SimulateSyncIoLoad()
	orderItemsCnt := rand.Intn(20) + 1 // Item count range: [1, 20]
	for range orderItemsCnt {
		util.SimulateSyncIoLoad()  // put Item
		util.SimulateCpuLoad(1000) // Merge SKU Items
		util.SimulateSyncIoLoad()  // put SKU
	}
}

func CompleteOrder() {
	util.SimulateSyncIoLoad()
}
