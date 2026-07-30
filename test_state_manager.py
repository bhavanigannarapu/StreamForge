from state_manager import StateManager

manager = StateManager()

manager.update_temperature("Truck-101", 5)
manager.update_temperature("Truck-101", 7)
manager.update_temperature("Truck-101", 8)

manager.update_temperature("Truck-102", 3)
manager.update_temperature("Truck-102", 4)

print("Truck-101 Latest:", manager.get_latest_temperature("Truck-101"))
print("Truck-101 Average:", manager.get_running_average("Truck-101"))

print("Truck-102 Latest:", manager.get_latest_temperature("Truck-102"))
print("Truck-102 Average:", manager.get_running_average("Truck-102"))