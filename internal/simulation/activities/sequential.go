package activities

import (
	"AsyncDB/pkg/util"
	"math/rand"
)

type SequentialSimulator struct {
	config *Config
}

func NewSequentialSimulator(config *Config) *SequentialSimulator {
	return &SequentialSimulator{
		config: config,
	}
}

func (s *SequentialSimulator) ValidateCheckout() {
	// This function was not implemented in the original BroadLeaf use-case
}

func (s *SequentialSimulator) ValidateAvailability() {
	orderItemsCnt := s.config.OrderItemsCnt
	for range orderItemsCnt {
		util.SimulateSyncIoLoad() // Load to get the item availability
		util.SimulateCpuLoad(100) // Merge SKU Items
	}
	skuItemsCnt := s.config.SKUItemsCnt
	for range skuItemsCnt {
		util.SimulateSyncIoLoad() // Load to get the SKU availability
		util.SimulateCpuLoad(100) // Some operations on SKU Items
	}
}

func (s *SequentialSimulator) VerifyCustomer() {
	util.SimulateSyncIoLoad() // Load to get the customer details
	util.SimulateCpuLoad(100)
	appliedOffersCnt := s.config.AppliedOffersCnt
	for range appliedOffersCnt {
		isLimitedUse := rand.Intn(2) == 0
		if isLimitedUse {
			util.SimulateSyncIoLoad() // Get uses by customer
			util.SimulateCpuLoad(1000)
		}
	}
}

func (s *SequentialSimulator) ValidatePayment() {
	util.SimulateSyncIoLoad() // Get Order
	paymentsCnt := s.config.PaymentsCnt
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

func (s *SequentialSimulator) ValidateProductOption() {

}

func (s *SequentialSimulator) RecordOffer() {
	util.SimulateSyncIoLoad() // Get Order
	util.SimulateCpuLoad(10000)
}

func (s *SequentialSimulator) CommitTax() {
	util.SimulateSyncIoLoad() // Get Order
	util.SimulateSyncIoLoad()
}

func (s *SequentialSimulator) DecrementInventory() {
	util.SimulateSyncIoLoad()
	orderItemsCnt := s.config.OrderItemsCnt
	for range orderItemsCnt {
		util.SimulateSyncIoLoad()  // put Item
		util.SimulateCpuLoad(1000) // Merge SKU Items
		util.SimulateSyncIoLoad()  // put SKU
	}
}

func (s *SequentialSimulator) CompleteOrder() {
	util.SimulateSyncIoLoad()
}
