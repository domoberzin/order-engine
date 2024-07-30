package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type Engine struct {
	initialised bool
	DispatcherChan chan order
}

type order struct {
	in   inputData
	executedCount uint32
	time int64
	response chan response
	isBuy bool
}

type matchEngineCmd struct {
	action string
	order  inputData
	resp chan response
	isBuy bool
}

type orderListCmd struct {
	action string
	order  inputData
	response chan response
	timestamp int64
	hasLock bool
}


type checkTime struct {
	action string
	time int64
	resp chan int64
}

type BuyOrder struct {
	price uint32
	in inputData
}

type BuyNode struct {
	order *BuyOrder
	next  *BuyNode
}

type SortedBuyLinkedList struct {
	head *BuyNode
}

type SellOrder struct {
	price uint32
	in inputData
}

type SellNode struct {
	order *SellOrder
	next  *SellNode
}

type inputData struct {
	orderType  inputType
	orderId    uint32
	price      uint32
	count      uint32
	executedCount uint32
	instrument string
}


type SortedSellLinkedList struct {
	head *SellNode
}


func checkLastAdds(cmds chan checkTime, ready chan<- struct{}) {
	timestampB := GetCurrentTimestamp()
	timestampS := GetCurrentTimestamp()

	ready <- struct{}{}

	for cmd := range cmds {
		switch cmd.action {
			case "checkB1":
				cmd.resp <- timestampB
			case "checkS1":
				cmd.resp <- timestampS
			case "checkBforUpdateS":
				cmd.resp <- timestampB
			case "checkSforUpdateB":
				cmd.resp <- timestampS
			case "UpdateS":
				timestampS = GetCurrentTimestamp()
				cmd.resp <- timestampS
			case "UpdateB":
				timestampB = GetCurrentTimestamp()
				cmd.resp <- timestampB
		}
	}
}

func buyOrderListManager(cmds chan orderListCmd, ready chan<- struct{}, orderMapCmds chan<- orderMapCmd, sellChan chan orderListCmd, instrLock chan struct{}, timeCheckChan chan checkTime) {
	buyOrders := &SortedBuyLinkedList{}

	ready <- struct{}{}
	for cmd := range cmds {
		switch cmd.action {
		case "add":
			<- instrLock
			timeCh := make(chan int64)

			check := checkTime{
				action: "checkSforUpdateB",
				time: cmd.timestamp,
				resp: timeCh,
			}

			timeCheckChan <- check
			reply := <- timeCh

			if (reply > cmd.timestamp) {
				sellChan <- orderListCmd{
					action: "match",
					order: cmd.order,
					response: cmd.response,
					timestamp: GetCurrentTimestamp(),
				}
			} else {

			newOrder := &BuyOrder {
				price: cmd.order.price,
				in: cmd.order,
			}
			newNode := &BuyNode{order: newOrder}
			if buyOrders.head == nil || buyOrders.head.order.price < cmd.order.price {
				newNode.next = buyOrders.head
				buyOrders.head = newNode
			} else {
				current := buyOrders.head
				for current.next != nil && current.next.order.price >= cmd.order.price {
					current = current.next
				}
				newNode.next = current.next
				current.next = newNode
			}
			orderMapCmds <- orderMapCmd{
				action: "add",
				orderID: cmd.order.orderId,
				details: truncatedOrderDetails{
					orderType: cmd.order.orderType,
					Instrument: cmd.order.instrument,
				},
			}
			ts := GetCurrentTimestamp()
			check = checkTime{
				action: "UpdateB",
				time: ts,
				resp: timeCh,
			}

			timeCheckChan <- check
			ts = <- timeCh
			close(timeCh)
			printData := input{
				orderType: cmd.order.orderType,
				orderId: cmd.order.orderId,
				price: cmd.order.price,
				count: cmd.order.count,
				instrument: cmd.order.instrument,
			}

			outputOrderAdded(printData, ts)
			cmd.response <- response{
				Status: "Success",
				Message: "Order processed",
				OrderID: cmd.order.orderId,
			}
		}
			instrLock <- struct{}{}
		case "remove":
			orderToBeRemoved := cmd.order
			found := false
			current := buyOrders.head
			var prev *BuyNode = nil
			
			for current != nil {
				b := &current.order.in
				if b.orderId == orderToBeRemoved.orderId && b.count > 0 {
					if prev == nil {
						buyOrders.head = current.next
					} else {
						prev.next = current.next
					}
					found = true
					orderMapCmds <- orderMapCmd{
						action: "delete",
						orderID: b.orderId,
						details: truncatedOrderDetails{
							orderType: b.orderType,
							Instrument: b.instrument,
						},
					}
					break
				} else {
					prev = current
					current = current.next
				}
			}

			printData := input{
				orderType: cmd.order.orderType,
				orderId: cmd.order.orderId,
			}

			outputOrderDeleted(printData, found, GetCurrentTimestamp())
			cmd.response <- response{
				Status: "Success",
				Message: "Order processed",
				OrderID: cmd.order.orderId,
			}
		case "match":
			ts := GetCurrentTimestamp()
			current := buyOrders.head
			var prev *BuyNode = nil
			
			for current != nil {
				b := &current.order.in

				if b.price >= cmd.order.price {
					if b.count > cmd.order.count {
						b.count -= cmd.order.count
						b.executedCount++
						oldRemainingCount := cmd.order.count
						cmd.order.count = 0
						ts = GetCurrentTimestamp()
						outputOrderExecuted(b.orderId, cmd.order.orderId, b.executedCount, b.price, oldRemainingCount, ts)
						current.order.in = inputData{
							orderId: b.orderId,
							orderType: b.orderType,
							instrument: b.instrument,
							price: b.price,
							count: b.count,
							executedCount: b.executedCount,
						}
						break
					} else {
						cmd.order.count -= b.count
						b.executedCount++
						ts = GetCurrentTimestamp()
						outputOrderExecuted(b.orderId, cmd.order.orderId, b.executedCount, b.price, b.count, ts)
						if prev == nil {
							buyOrders.head = current.next
						} else {
							prev.next = current.next
						}
						orderMapCmds <- orderMapCmd{
							action: "delete",
							orderID: b.orderId,
							details: truncatedOrderDetails{
								orderType: b.orderType,
								Instrument: b.instrument,
							},
						}
						current = current.next
						if cmd.order.count <= 0 {
							break
						}
						continue
					}
				} else {
					prev = current
					current = current.next
				}

				if cmd.order.count <= 0 {
					break
				}
			}
			if (cmd.order.count > 0) {
				timeCh := make(chan int64)

				check := checkTime{
					action: "checkB1",
					time: cmd.timestamp,
					resp: timeCh,
				}
	
				timeCheckChan <- check
				reply := <- timeCh
				close(timeCh)
				sellChan <- orderListCmd{
					action: "add",
					order: cmd.order,
					response: cmd.response,
					timestamp: reply,
				}
			} else {
				cmd.response <- response{
					Status: "Success",
					Message: "Order processed",
					OrderID: cmd.order.orderId,
				}
			}
	}
	}
}


func sellOrderListManager(cmds chan orderListCmd, ready chan<- struct{}, orderMapCmds chan<- orderMapCmd, buyChan chan orderListCmd, instrLock chan struct{}, timeCheckChan chan checkTime) {
	sellOrders := &SortedSellLinkedList{}

	ready <- struct{}{}
	for cmd := range cmds {
		switch cmd.action {
		case "add":
			<- instrLock
			timeCh := make(chan int64)

			check := checkTime{
				action: "checkBforUpdateS",
				time: cmd.timestamp,
				resp: timeCh,
			}

			timeCheckChan <- check
			reply := <- timeCh

			if (reply > cmd.timestamp) {
				buyChan <- orderListCmd{
					action: "match",
					order: cmd.order,
					response: cmd.response,
					timestamp: GetCurrentTimestamp(),
				}
			} else {
			newOrder := &SellOrder {
				price: cmd.order.price,
				in: cmd.order,
			}
			newNode := &SellNode{order: newOrder}
			if sellOrders.head == nil || sellOrders.head.order.price > cmd.order.price {
				newNode.next = sellOrders.head
				sellOrders.head = newNode
			} else {
				current := sellOrders.head
				for current.next != nil && current.next.order.price <= cmd.order.price {
					current = current.next
				}
				newNode.next = current.next
				current.next = newNode
			}

			orderMapCmds <- orderMapCmd{
				action: "add",
				orderID: cmd.order.orderId,
				details: truncatedOrderDetails{
					orderType: cmd.order.orderType,
					Instrument: cmd.order.instrument,
				},
			}
			ts := GetCurrentTimestamp()
			check = checkTime{
				action: "UpdateS",
				time: ts,
				resp: timeCh,
			}

			timeCheckChan <- check
			ts = <- timeCh
			close(timeCh)
			printData := input{
				orderType: cmd.order.orderType,
				orderId: cmd.order.orderId,
				price: cmd.order.price,
				count: cmd.order.count,
				instrument: cmd.order.instrument,
			}

			outputOrderAdded(printData, ts)
			cmd.response <- response{
				Status: "Success",
				Message: "Order processed",
				OrderID: cmd.order.orderId,
			}
		}
			instrLock <- struct{}{}
		case "remove":
			found := false
			orderToBeRemoved := cmd.order
			current := sellOrders.head
			var prev *SellNode = nil
			
			for current != nil {
				b := &current.order.in
				if b.orderId == orderToBeRemoved.orderId && b.count > 0 {
					if prev == nil {
						sellOrders.head = current.next
					} else {
						prev.next = current.next
					}
					found = true
					orderMapCmds <- orderMapCmd{
						action: "delete",
						orderID: b.orderId,
						details: truncatedOrderDetails{
							orderType: b.orderType,
							Instrument: b.instrument,
						},
					}
					break
				} else {
					prev = current
					current = current.next
				}
			}

			printData := input{
				orderType: cmd.order.orderType,
				orderId: cmd.order.orderId,
			}

			outputOrderDeleted(printData, found, GetCurrentTimestamp())

			cmd.response <- response{
				Status: "Success",
				Message: "Order processed",
				OrderID: cmd.order.orderId,
			}
		case "match":
			ts := GetCurrentTimestamp()
			current := sellOrders.head
			var prev *SellNode = nil
			for current != nil {
				b := &current.order.in

				if b.price <= cmd.order.price {
					if b.count > cmd.order.count {
						b.count -= cmd.order.count
						b.executedCount++
						oldRemainingCount := cmd.order.count
						cmd.order.count = 0
						ts = GetCurrentTimestamp()
						outputOrderExecuted(b.orderId, cmd.order.orderId, b.executedCount, b.price, oldRemainingCount, ts)
						current.order.in = inputData{
							orderId: b.orderId,
							orderType: b.orderType,
							instrument: b.instrument,
							price: b.price,
							count: b.count,
							executedCount: b.executedCount,
						}
						break
					} else {
						cmd.order.count -= b.count
						b.executedCount++
						ts = GetCurrentTimestamp()
						outputOrderExecuted(b.orderId, cmd.order.orderId, b.executedCount, b.price, b.count, ts)
						if prev == nil {
							sellOrders.head = current.next
						} else {
							prev.next = current.next
						}
						orderMapCmds <- orderMapCmd{
							action: "delete",
							orderID: b.orderId,
							details: truncatedOrderDetails{
								orderType: b.orderType,
								Instrument: b.instrument,
							},
						}
						current = current.next

						if cmd.order.count <= 0 {
							break
						}
						continue
					}
				} else {
					prev = current
					current = current.next
				}

				if cmd.order.count <= 0 {
					break
				}
			}

			if (cmd.order.count > 0) {
				timeCh := make(chan int64)

				check := checkTime{
					action: "checkS1",
					time: cmd.timestamp,
					resp: timeCh,
				}
	
				timeCheckChan <- check
				reply := <- timeCh
				close(timeCh)
					buyChan <- orderListCmd{
						action: "add",
						order: cmd.order,
						response: cmd.response,
						timestamp: reply,
					}
			} else {

				cmd.response <- response{
					Status: "Success",
					Message: "Order processed",
					OrderID: cmd.order.orderId,
				}
			}

		}
	}
}

func matchingEngine(cmds chan matchEngineCmd, orderMapCmds chan<- orderMapCmd) {
	buyCmds := make(chan orderListCmd, 1000)
	sellCmds := make(chan orderListCmd, 1000)
	instrLock := make(chan struct{}, 1)

	timeCheck := make(chan checkTime)

	ready := make(chan struct{}, 3)
	go checkLastAdds(timeCheck, ready)
	go buyOrderListManager(buyCmds, ready, orderMapCmds, sellCmds, instrLock, timeCheck)
	go sellOrderListManager(sellCmds, ready, orderMapCmds, buyCmds, instrLock, timeCheck)

	<-ready
	<-ready
	<-ready
	instrLock <- struct{}{}

	for cmd := range cmds {
		switch cmd.action {
		case "addBuy":
			sellCmds <- orderListCmd{
				action: "match",
				order: cmd.order,
				response: cmd.resp,
			}

		case "addSell":
			buyCmds <- orderListCmd{
				action: "match",
				order: cmd.order,
				response: cmd.resp,
			}

		case "cancel":
			if cmd.isBuy {
				buyCmds <- orderListCmd{
					action: "remove",
					order: cmd.order,
					response: cmd.resp,
				}
			} else {
				sellCmds <- orderListCmd{
					action: "remove",
					order: cmd.order,
					response: cmd.resp,
				}
			}
		}
	
	}
}

func instrumentHandler(dataStream <-chan order, matchEngineChan chan matchEngineCmd) {
	for ord := range dataStream {
		cmd := matchEngineCmd{
			order:  ord.in,
			resp: ord.response,
			isBuy: ord.isBuy,
		}

		switch ord.in.orderType {
		case inputBuy:
			cmd.action = "addBuy"
		case inputSell:
			cmd.action = "addSell"
		case inputCancel:
			cmd.action = "cancel"
		}
		matchEngineChan <- cmd
	}
}

func NewEngine() *Engine {
	engine := &Engine{DispatcherChan: make(chan order)}
	go dispatcher(engine)
	return engine
}

type truncatedOrderDetails struct{
	orderType    inputType
	Instrument   string
}

type orderMapCmd struct {
	action     string
	orderID    uint32
	details    truncatedOrderDetails
	in		   inputData
	responseCh chan<- orderMapResponse
}

type orderMapResponse struct {
	details truncatedOrderDetails
	found   bool
}

func orderIDMapManager(cmds <-chan orderMapCmd) {
	orderIdMap := make(map[uint32]truncatedOrderDetails)

	for cmd := range cmds {
		switch cmd.action {
		case "add":
			orderIdMap[cmd.orderID] = cmd.details
		case "delete":
			delete(orderIdMap, cmd.orderID)
		case "get":
			if details, found := orderIdMap[cmd.orderID]; found {
				cmd.responseCh <- orderMapResponse{details: details, found: true}
			} else {
				printData := input{
					orderType: cmd.in.orderType,
					orderId: cmd.in.orderId,
					price: cmd.in.price,
					count: cmd.in.count,
					instrument: cmd.in.instrument,
				}
				outputOrderDeleted(printData, false, GetCurrentTimestamp())
				cmd.responseCh <- orderMapResponse{found: false}
			}
		}
	}
}

type response struct {
	Status  string
	Message string
	OrderID uint32 
}

func dispatcher(engine *Engine) {
	orderMapCmds := make(chan orderMapCmd, 1000)
	go orderIDMapManager(orderMapCmds)
	instrumentHandlers := make(map[string]chan order)
	instrumentMatchingEngines := make(map[string]chan matchEngineCmd)

	for ord := range engine.DispatcherChan {
		cont := true
		if ord.in.orderType == inputCancel {
			ch := make(chan orderMapResponse)
			orderMapCmds <- orderMapCmd{
				action: "get",
				orderID: ord.in.orderId,
				responseCh: ch,
				in: ord.in,
			}
			resp := <-ch
			close(ch)
			if resp.found {
				ord.in.instrument = resp.details.Instrument
				ord.isBuy = resp.details.orderType == inputBuy
				cont = true
			} else {
				tempResponse := response{
					Status: "Failed",
					Message: "Order not found",
					OrderID: ord.in.orderId,
				}
				ord.response <- tempResponse
				cont = false
			}
		}

		if (!cont) {
			continue;
		}
		if _, ok := instrumentHandlers[ord.in.instrument]; !ok {
			instrChan := make(chan order)
			instrumentMatchingEngineChan := make(chan matchEngineCmd)
			go matchingEngine(instrumentMatchingEngineChan, orderMapCmds)
			go instrumentHandler(instrChan, instrumentMatchingEngineChan)
			instrumentHandlers[ord.in.instrument] = instrChan
			instrumentMatchingEngines[ord.in.instrument] = instrumentMatchingEngineChan
		}

		instrChan := instrumentHandlers[ord.in.instrument]
		instrChan <- ord
	}
}


func (e *Engine) accept(ctx context.Context, conn net.Conn) {
	if !e.initialised {
		e.initialised = true
		e.DispatcherChan = make(chan order)
		go dispatcher(e)
	}

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	go handleConn(conn, e)
}



func handleConn(conn net.Conn, engine *Engine) {
	defer conn.Close()
	responseCh := make(chan response)

	for {
		in, err := readInput(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			return
		}

		modifiedInput := inputData{
			orderType: in.orderType,
			orderId: in.orderId,
			price: in.price,
			count: in.count,
			executedCount: 0,
			instrument: in.instrument,
		}

		timeNow := GetCurrentTimestamp()

		engine.DispatcherChan <- order{in: modifiedInput, time: timeNow, response: responseCh, isBuy: false}

		<-responseCh
	}
	close(responseCh)
}


func GetCurrentTimestamp() int64 {
	return time.Now().UnixNano()
}