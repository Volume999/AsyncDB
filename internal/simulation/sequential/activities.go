package sequentialwf

import (
	"AsyncDB/internal/simulation"
	"AsyncDB/pkg/util"
	"math/rand"
)

func ValidateCheckout(config *simulation.Config) {
	// This function was not implemented in the original BroadLeaf use-case
}

func ValidateAvailability(config *simulation.Config) {
	orderItemsCnt := config.OrderItemsCnt
	for range orderItemsCnt {
		util.SimulateSyncIoLoad() // Load to get the item availability
		util.SimulateCpuLoad(100) // Merge SKU Items
	}
	skuItemsCnt := config.SKUItemsCnt
	for range skuItemsCnt {
		util.SimulateSyncIoLoad() // Load to get the SKU availability
		util.SimulateCpuLoad(100) // Some operations on SKU Items
	}
}

func VerifyCustomer(config *simulation.Config) {
	util.SimulateSyncIoLoad() // Load to get the customer details
	util.SimulateCpuLoad(100)
	appliedOffersCnt := config.AppliedOffersCnt
	for range appliedOffersCnt {
		isLimitedUse := rand.Intn(2) == 0
		if isLimitedUse {
			util.SimulateSyncIoLoad() // Get uses by customer
			util.SimulateCpuLoad(1000)
		}
	}
}

func ValidatePayment(config *simulation.Config) {
	util.SimulateSyncIoLoad() // Get Order
	paymentsCnt := config.PaymentsCnt
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

func ValidateProductOption(config *simulation.Config) {

}

func RecordOffer(config *simulation.Config) {
	util.SimulateSyncIoLoad() // Get Order
	util.SimulateCpuLoad(10000)
}

func CommitTax(config *simulation.Config) {
	util.SimulateSyncIoLoad() // Get Order
	util.SimulateSyncIoLoad()
}

func DecrementInventory(config *simulation.Config) {
	util.SimulateSyncIoLoad()
	orderItemsCnt := config.OrderItemsCnt
	for range orderItemsCnt {
		util.SimulateSyncIoLoad()  // put Item
		util.SimulateCpuLoad(1000) // Merge SKU Items
		util.SimulateSyncIoLoad()  // put SKU
	}
}

func CompleteOrder(config *simulation.Config) {
	util.SimulateSyncIoLoad()
}
