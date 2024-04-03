package activities

import (
	"AsyncDB/pkg/util"
	"math/rand"
)

type SequentialSimulator struct {
	config *Config
	disk   DiskAccessSimulator
}

func NewSequentialSimulator(config *Config, disk DiskAccessSimulator) *SequentialSimulator {
	return &SequentialSimulator{
		config: config,
		disk:   disk,
	}
}

func (s *SequentialSimulator) ValidateCheckout() {
	// This function was not implemented in the original BroadLeaf use-case
}

func (s *SequentialSimulator) ValidateAvailability() {
	orderItemsCnt := s.config.OrderItemsCnt
	for range orderItemsCnt {
		s.disk.SimulateDiskAccess() // Load to get the item availability
		util.SimulateCpuLoad(100)   // Merge SKU Items
	}
	skuItemsCnt := s.config.SKUItemsCnt
	for range skuItemsCnt {
		s.disk.SimulateDiskAccess() // Load to get the SKU availability
		util.SimulateCpuLoad(100)   // Some operations on SKU Items
	}
}

func (s *SequentialSimulator) VerifyCustomer() {
	s.disk.SimulateDiskAccess() // Load to get the customer details
	util.SimulateCpuLoad(100)
	appliedOffersCnt := s.config.AppliedOffersCnt
	for range appliedOffersCnt {
		isLimitedUse := rand.Intn(2) == 0
		if isLimitedUse {
			s.disk.SimulateDiskAccess() // Get uses by customer
			util.SimulateCpuLoad(1000)
		}
	}
}

func (s *SequentialSimulator) ValidatePayment() {
	s.disk.SimulateDiskAccess() // Get Order
	paymentsCnt := s.config.PaymentsCnt
	for range paymentsCnt {
		isActive := rand.Intn(10) < 4
		if isActive {
			s.disk.SimulateDiskAccess() // Make new transaction
			util.SimulateCpuLoad(10000)
			s.disk.SimulateDiskAccess()
			s.disk.SimulateDiskAccess()
		}
	}
}

func (s *SequentialSimulator) ValidateProductOption() {

}

func (s *SequentialSimulator) RecordOffer() {
	s.disk.SimulateDiskAccess() // Get Order
	util.SimulateCpuLoad(10000)
}

func (s *SequentialSimulator) CommitTax() {
	s.disk.SimulateDiskAccess() // Get Order
	s.disk.SimulateDiskAccess()
}

func (s *SequentialSimulator) DecrementInventory() {
	s.disk.SimulateDiskAccess()
	orderItemsCnt := s.config.OrderItemsCnt
	for range orderItemsCnt {
		s.disk.SimulateDiskAccess() // put Item
		util.SimulateCpuLoad(1000)  // Merge SKU Items
		s.disk.SimulateDiskAccess() // put SKU
	}
}

func (s *SequentialSimulator) CompleteOrder() {
	s.disk.SimulateDiskAccess()
}
