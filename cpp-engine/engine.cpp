#include <iostream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <queue>
#include <memory>
#include <cstring>

#include "io.hpp"
#include "engine.hpp"

struct Order
{
	uint32_t order_id;
	uint32_t price;
	uint32_t count;
	bool is_buy;
	uint32_t matched;
	long long timestamp;
	char* instrument;

	Order() : order_id(0), price(0), count(0), is_buy(false), matched(0), timestamp(0), instrument(nullptr) {}

	Order(uint32_t order_id, uint32_t price, uint32_t count, bool is_buy, 
		  uint32_t matched_initial, long long timestamp, char* instrument)
		: order_id(order_id), price(price), count(count), is_buy(is_buy),
		  matched(matched_initial), timestamp(timestamp), instrument(instrument) {}

	Order(const Order& other) : order_id(other.order_id), price(other.price), count(other.count), 
	is_buy(other.is_buy), matched(other.matched), timestamp(other.timestamp), instrument(other.instrument)
	{
		if (other.instrument != nullptr) {
			instrument = new char[strlen(other.instrument) + 1];
			strcpy(instrument, other.instrument);
		} else {
			instrument = nullptr;
		}
	}

	Order& operator=(const Order& other) {
		if (this != &other) {
			order_id = other.order_id;
			price = other.price;
			count = other.count;
			is_buy = other.is_buy;
			matched = other.matched;
			timestamp = other.timestamp;
			instrument = other.instrument;
		}
		return *this;
	}
};

struct BuyNode {
	uint32_t price;
	std::shared_ptr<Order> order;
	BuyNode *next;
	BuyNode *prev;
	std::mutex mtx;

	BuyNode(): price{0}, order{nullptr}, next{nullptr}, prev{nullptr}, mtx{} {};
	BuyNode(std::shared_ptr<Order> order): price{order->price}, order{order}, next{nullptr}, prev{nullptr}, mtx{} {};
};

struct SellNode {
	uint32_t price;
	std::shared_ptr<Order> order;
	SellNode *next;
	SellNode *prev;
	std::mutex mtx;
   
	SellNode(): price{0}, order{nullptr}, next{nullptr}, prev{nullptr}, mtx{} {};
	SellNode(std::shared_ptr<Order> order): price{order->price}, order{order}, next{nullptr}, prev{nullptr}, mtx{} {};
};

std::mutex orderIdMapMutex;
std::unordered_map<uint32_t, std::shared_ptr<Order>> orderIdMap;
struct InstrumentStuff {
	std::mutex mtx;
	SellNode sellStart;
	BuyNode buyStart;
	SellNode sellEnd;
	BuyNode buyEnd;

	InstrumentStuff(): mtx{}, sellStart{SellNode()}, buyStart{BuyNode()}, sellEnd{SellNode()}, buyEnd{BuyNode()} {
		sellStart.next = &sellEnd;
		buyStart.next = &buyEnd;
		sellEnd.prev = &sellStart;
		buyEnd.prev = &buyStart;
	}

	InstrumentStuff(const InstrumentStuff&) = delete;
	InstrumentStuff& operator=(const InstrumentStuff&) = delete;

	void handleSellOrdersV2(std::shared_ptr<Order> inputOrder) {
		uint32_t count = inputOrder->count;
		std::unique_lock lock1(mtx, std::defer_lock);
		std::unique_lock lock2(buyStart.mtx, std::defer_lock);
		std::lock(lock1, lock2);
		// mtx.lock();
		// buyStart.mtx.lock();

		uint32_t price = inputOrder->price;
		BuyNode* current = buyStart.next;
		std::vector<BuyNode*> matches;

		while (current != nullptr && count > 0) {
			current->mtx.lock();
			if (current == &buyEnd) {
				// we have matched the end of the queue, add this in to manage later on.
				matches.push_back(current);
				break;
			}

			uint32_t count2 = current->order->count;
			// uint32_t count2 = current->order->count.load();
			
			if (current->price < price) {
				// break;
				current->mtx.unlock();
				break;
			}

			matches.push_back(current);

			count = count2 >= count ? 0 : count - count2;
			
			
			current = current->next;

		}


		// lock.unlock();
		if (count != 0) {
			sellStart.mtx.lock();
		}

		// mtx.unlock();
		lock1.unlock();

		uint32_t currCount = inputOrder->count;
		// uint32_t currCount = inputOrder->count.load();

		for (auto it = matches.begin(); it != matches.end(); it++) {
			BuyNode* current = *it;


			if (current == &buyEnd) {
				current->mtx.unlock();
				break;
			}


			// if (current->order->count.load() == 0) {
			if (current->order->count == 0) {
				current->mtx.unlock();
				continue;
			}

			BuyNode* tempNext = current->next;
			BuyNode* tempPrev = current->prev;
			uint32_t txnPrice = current->price;
			uint32_t dealt = std::min(currCount, current->order->count);
			// uint32_t dealt = std::min(currCount, current->order->count.load());
			uint32_t match = current->order->matched;

			current->order->count -= dealt;
			currCount -= dealt;
			Output::OrderExecuted(current->order->order_id, inputOrder->order_id, match, txnPrice, dealt, getCurrentTimestamp());
			if (current->order->count == 0) {
			// if (current->order->count.load() == 0) {
				if (tempPrev != &buyStart) {
					tempPrev->mtx.lock();
				}
				{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap.erase(current->order->order_id);
						// Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());
				}
				tempNext->prev = tempPrev;
				tempPrev->next = tempNext;
				current->next = nullptr;
				current->prev = nullptr;
				current->mtx.unlock();
				delete current;
				if (tempPrev != &buyStart) {
					tempPrev->mtx.unlock();
				}
			} else {
				current->order->matched += 1;
				current->mtx.unlock();
				// Output::OrderExecuted(current->order->order_id, inputOrder->order_id, match, txnPrice, dealt, getCurrentTimestamp());
			}
		}

		// buyStart.mtx.unlock();
		lock2.unlock();


		if (currCount > 0) {
			// need to add and we have the necessary mutexes already.
			SellNode* current = sellStart.next;
			SellNode* prev = &sellStart;
			while (current != nullptr) {
				current->mtx.lock();
				if (current == &sellEnd) {
					break;
				}
				if (price >= current->price) {

					SellNode* temp = current->prev;
					prev = current;
					temp->mtx.unlock();
					current = current->next;
				} else {
					break;
				}
			}

			inputOrder->count = currCount;
			SellNode* input = new SellNode(inputOrder);
			input->mtx.lock();

			if (prev == &sellStart && current == &sellEnd) {
				sellStart.next = input;
				sellEnd.prev = input;
				input->next = &sellEnd;
				input->prev = &sellStart;
				{
					std::lock_guard lkO(orderIdMapMutex);
					orderIdMap[inputOrder->order_id] = inputOrder;
					Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());

				}
				(&sellEnd)->mtx.unlock();
				input->prev->mtx.unlock();
				input->mtx.unlock();
			} else {
				if (current == nullptr) {
					input->prev = (&sellEnd)->prev;
					input->next = &sellEnd;
					(&sellEnd)->prev = input;
					{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap[inputOrder->order_id] = inputOrder;
						Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());

					}
					(&sellEnd)->mtx.unlock();
					input->prev->mtx.unlock();
					input->mtx.unlock();

				} else {

					input->prev = current->prev;
					input->next = current;
					prev->next = input;
					current->prev = input;
					{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap[inputOrder->order_id] = inputOrder;
						Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());
					}
					prev->mtx.unlock();
					input->mtx.unlock();
					current->mtx.unlock();
				}
			}
		// SyncCerr {} << "Sell stuff unlocked by sell order: " << inputOrder->order_id << std::endl;
		// sellMtx.unlock();
		// sellStart.mtx.unlock();
		}
	}

	void handleBuyOrdersV2(std::shared_ptr<Order> inputOrder) {
		// uint32_t count = inputOrder->count.load();
		uint32_t count = inputOrder->count;
		std::unique_lock lock1(mtx, std::defer_lock);
		std::unique_lock lock2(sellStart.mtx, std::defer_lock);
		// std::unique_lock lock3(buyStart.mtx, std::defer_lock); // may be required later, may not be
		std::lock(lock1, lock2);
		// mtx.lock();
		// sellStart.mtx.lock();

		uint32_t price = inputOrder->price;
		SellNode* current = sellStart.next;
		std::vector<SellNode*> matches;

		while (current != nullptr && count > 0) {
			current->mtx.lock();
			if (current == &sellEnd) {
				matches.push_back(current);
				break;
			}
			// uint32_t count2 = current->order->count.load();
			uint32_t count2 = current->order->count;
			if (current->price > price) {
				current->mtx.unlock();
				break;
			}
			matches.push_back(current);
			count = count2 >= count ? 0 : count - count2;
			current = current->next;
		}

		if (count != 0) {
			buyStart.mtx.lock();
		}

		// mtx.unlock();
		lock1.unlock();
		// uint32_t currCount = inputOrder->count.load();
		uint32_t currCount = inputOrder->count;
		for (auto it = matches.begin(); it != matches.end(); it++) {
			
			SellNode* current = *it;

			if (current == &sellEnd) {
				current->mtx.unlock();
				break;
			}

			if (current->order->count == 0) {
			// if (current->order->count.load() == 0) {
				current->mtx.unlock();
				continue;
			}

			SellNode* tempNext = current->next;
			SellNode* tempPrev = current->prev;
			uint32_t txnPrice = current->price;
			// uint32_t dealt = std::min(currCount, current->order->count.load());
			uint32_t dealt = std::min(currCount, current->order->count);
			uint32_t match = current->order->matched;
			current->order->count -= dealt;
			currCount -= dealt;
			Output::OrderExecuted(current->order->order_id, inputOrder->order_id, match, txnPrice, dealt, getCurrentTimestamp());
			if (current->order->count == 0) {
			// if (current->order->count.load() == 0) {
				if (tempPrev != &sellStart) {
					tempPrev->mtx.lock();
				}
				{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap.erase(current->order->order_id);
						// Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());

				}
				tempNext->prev = tempPrev;
				tempPrev->next = tempNext;
				current->next = nullptr;
				current->prev = nullptr;
				current->mtx.unlock();
				delete current;
				if (tempPrev != &sellStart) {
					tempPrev->mtx.unlock();
				}
			} else {
				current->order->matched += 1;
				current->mtx.unlock();
			}
		}

		lock2.unlock();

		if (currCount > 0) {
			// need to add and we have the necessary mutexes already.
			BuyNode* current = buyStart.next;
			BuyNode* prev = &buyStart;
			while (current != nullptr) {
				current->mtx.lock();
				if (current == &buyEnd) {
					break;
				}
				if (price <= current->price) {

					BuyNode* temp = current->prev;
					prev = current;
					temp->mtx.unlock();
					current = current->next;
				} else {
					break;
				}
			}
			
			inputOrder->count = currCount;
			BuyNode* input = new BuyNode(inputOrder);
			input->mtx.lock();


			if (prev == &buyStart && current == &buyEnd) {
				buyStart.next = input;
				buyEnd.prev = input;
				input->next = &buyEnd;
				input->prev = &buyStart;
				{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap[inputOrder->order_id] = inputOrder;
						Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());

				}
				// Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());
				(&buyEnd)->mtx.unlock();
				input->prev->mtx.unlock();
				input->mtx.unlock();
			} else {

				if (current == nullptr) {
					input->prev = (&buyEnd)->prev;
					input->next = &buyEnd;
					(&buyEnd)->prev = input;
					{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap[inputOrder->order_id] = inputOrder;
						Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());
					}
					(&buyEnd)->mtx.unlock();
					input->prev->mtx.unlock();
					input->mtx.unlock();

				} else {

					input->prev = current->prev;
					input->next = current;
					prev->next = input;
					current->prev = input;
					{
						std::lock_guard lkO(orderIdMapMutex);
						orderIdMap[inputOrder->order_id] = inputOrder;
						Output::OrderAdded(inputOrder->order_id, inputOrder->instrument, inputOrder->price, currCount, !inputOrder->is_buy, getCurrentTimestamp());
					}
					prev->mtx.unlock();
					input->mtx.unlock();
					current->mtx.unlock();
				}
			}
		}
	}

	bool handleCancelOrders(std::shared_ptr<Order> inputOrder) {
		if (inputOrder->is_buy) {
			std::unique_lock lock(buyStart.mtx);
			// buyStart.mtx.lock();
			BuyNode* current = buyStart.next;
			BuyNode* prev = &buyStart;
			bool found = false;

			while (current != nullptr) {
				current->mtx.lock();

				if (current == &buyEnd) {
					if (prev != &buyStart) {
						prev->mtx.unlock();
					}
					current->mtx.unlock();
					break;
				}

				if (current->order->order_id == inputOrder->order_id && current->order->count != 0) {
					current->order->count = 0;
					found = true;
					BuyNode* next = current->next;
					next->mtx.lock();
					prev->next = next;
					next->prev = prev;
					current->next = nullptr;
					current->prev = nullptr;
					current->mtx.unlock();
					delete current;
					if (prev != &buyStart) {
						prev->mtx.unlock();
					}
					next->mtx.unlock();

					break;
				}

				BuyNode* next = current->next;
				if (prev != &buyStart) {
					prev->mtx.unlock();
				}
				prev = current;
				current = next;
			}

			Output::OrderDeleted(inputOrder->order_id, found, getCurrentTimestamp());
			// buyStart.mtx.unlock();
			return found;
		} else {
			std::unique_lock lock(sellStart.mtx);
			// sellStart.mtx.lock();
			SellNode* current = sellStart.next;
			SellNode* prev = &sellStart;
			bool found = false;

			while (current != nullptr) {
				current->mtx.lock();

				if (current == &sellEnd) {
					if (prev != &sellStart) {
						prev->mtx.unlock();
					}
					current->mtx.unlock();
					break;
				}

				if (current->order->order_id == inputOrder->order_id && current->order->count != 0) {
					current->order->count = 0;
					found = true;
					SellNode* next = current->next;
					next->mtx.lock();
					prev->next = next;
					next->prev = prev;
					current->next = nullptr;
					current->prev = nullptr;
					current->mtx.unlock();
					delete current;
					if (prev != &sellStart) {
						prev->mtx.unlock();
					}
					next->mtx.unlock();
					break;
				}

				SellNode* next = current->next;
				if (prev != &sellStart) {
					prev->mtx.unlock();
				}
				prev = current;
				current = next;
			}

			Output::OrderDeleted(inputOrder->order_id, found, getCurrentTimestamp());
			// sellStart.mtx.unlock();
			return found;
		}

		
	}
};

std::shared_mutex mainBookMutex;
std::unordered_map<std::string, InstrumentStuff*> mainBook;

InstrumentStuff* checkOrCreateInstrument(std::string instrument) {
	{
		std::shared_lock readLock(mainBookMutex);
		auto it = mainBook.find(instrument);
		if (it != mainBook.end()) {
			return it->second;
		}
	}

	std::unique_lock writeLock(mainBookMutex);
	auto it = mainBook.find(instrument);
	if (it == mainBook.end()) {
		mainBook[instrument] = new InstrumentStuff{};
	}

	return mainBook[instrument];
}

Order matchSellOrders(Order inputOrder) {
	InstrumentStuff* instrumentDetails = checkOrCreateInstrument(inputOrder.instrument);
	std::shared_ptr<Order> input = std::make_shared<Order>(inputOrder);
	instrumentDetails->handleBuyOrdersV2(input);
	return inputOrder;	
}


Order matchBuyOrders(Order inputOrder) {
	InstrumentStuff* instrumentDetails = checkOrCreateInstrument(inputOrder.instrument);
	std::shared_ptr<Order> input = std::make_shared<Order>(inputOrder);
	instrumentDetails->handleSellOrdersV2(input);
	return inputOrder;
}

void removeOrderFromOrderMap(uint32_t orderId) {
	{	
		// lock for the string::order pointer map, unique because we may need to unlock and re-lock.
		std::unique_lock lock(orderIdMapMutex);
		auto orderIdIt = orderIdMap.find(orderId);
		if (orderIdIt == orderIdMap.end() || orderIdIt->second->count == 0) {
			Output::OrderDeleted(orderId, false, getCurrentTimestamp());
			return;
		} 
		lock.unlock();
		InstrumentStuff* instrumentDetails = checkOrCreateInstrument(orderIdIt->second->instrument);
		std::shared_ptr<Order> input = std::make_shared<Order>(*orderIdIt->second);
		bool res = instrumentDetails->handleCancelOrders(input);

		if (res) {
			lock.lock();
			orderIdMap.erase(orderId);
			lock.unlock();
		}
	
	}
}



void Engine::accept(ClientConnection connection)
{
	auto thread = std::thread(&Engine::connection_thread, this, std::move(connection));
	thread.detach();
}

void Engine::connection_thread(ClientConnection connection)
{
	while(true)
	{
		ClientCommand input {};
		switch(connection.readInput(input))
		{
			case ReadResult::Error: SyncCerr {} << "Error reading input" << std::endl;
			case ReadResult::EndOfFile: return;
			case ReadResult::Success: break;
		}
		

		switch(input.type)
		{
				case input_buy: {
					auto localTime = getCurrentTimestamp();
					Order newOrder = {input.order_id, input.price, input.count, true, 1, localTime, input.instrument};
					matchSellOrders(newOrder);
					break;
				}
				case input_sell: {
					auto localTime = getCurrentTimestamp();
					Order newOrder = {input.order_id, input.price, input.count, false, 1, localTime, input.instrument};
					matchBuyOrders(newOrder);
					break;
				}

				case input_cancel: {
					removeOrderFromOrderMap(input.order_id);
					break;
				}
		}

	}
}