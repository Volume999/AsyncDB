package asyncwf

func validateCheckout() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func validateAvailability() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func verifyCustomer() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func validatePayment() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func recordOffer() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func commitTax() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func decrementInventory() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}

func completeOrder() chan<- struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		done <- struct{}{}
	}()
	return done
}
