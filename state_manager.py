class StateManager:
    def __init__(self):
        # Stores state for each truck
        self.truck_state = {}

    def update_temperature(self, truck_id, temperature):
        """
        Store latest temperature and update running average.
        """

        if truck_id not in self.truck_state:
            self.truck_state[truck_id] = {
                "latest_temperature": temperature,
                "count": 1,
                "sum_temperature": temperature,
                "running_average": temperature
            }
        else:
            truck = self.truck_state[truck_id]

            truck["latest_temperature"] = temperature
            truck["count"] += 1
            truck["sum_temperature"] += temperature
            truck["running_average"] = (
                truck["sum_temperature"] / truck["count"]
            )

    def get_latest_temperature(self, truck_id):
        if truck_id in self.truck_state:
            return self.truck_state[truck_id]["latest_temperature"]
        return None

    def get_running_average(self, truck_id):
        if truck_id in self.truck_state:
            return self.truck_state[truck_id]["running_average"]
        return None