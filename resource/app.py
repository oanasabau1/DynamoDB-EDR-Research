import boto3
import random
import datetime
import math
from decimal import Decimal

from boto3.dynamodb.conditions import Key

client = boto3.resource("dynamodb", region_name="eu-central-1")
events = client.Table("valentin-edr-events")
incidents = client.Table("valentin-edr-incidents")


event_type_list = ["rapid acc", "rapid dec", "dtc event"]
brake_status_list = ["applied", "released"]
seatbelt_status_list = ["locked", "unlocked"]

object_type_list = ["vehicle", "pedestrian", "cyclist", "tree", "sign"]
object_size_list = ["small", "medium", "large"]
object_class_list = ["car", "truck", "motorcycle"]


def measure_execution_time(f, executions=5):
    def wrapper(*args, **kwargs):
        sum = None
        for i in range(executions):
            start_time = datetime.datetime.now()
            f(*args, **kwargs)
            end_time = datetime.datetime.now()
            sum = sum + end_time - start_time if sum else end_time - start_time

        average = str(sum / executions).split(":").pop()
        print(f"Average execution time for {f.__name__} is [{average}] seconds")

    return wrapper


def get_vehicle_base_data():
    return {
        "base_speed": random.randint(50, 100),
        "base_fuel_level": random.randint(30, 80),
        "base_rpm": random.randint(1500, 3000),
        "base_throttle_position": random.randint(30, 70),
        "acceleration": {"x": random.uniform(-2, 2), "y": random.uniform(-1, 1), "z": 9.8},
        "location": {"latitude": random.uniform(37.0, 38.0), "longitude": random.uniform(-122.5, -122.0)},
    }


def generate_events(vehicle_id, vehicle_base_data, num_events=100):
    events_generated = []
    events_to_insert = []
    for i in range(num_events):
        event_timestamp = datetime.datetime.now() - datetime.timedelta(seconds=i * 10)
        event_timestamp_str = event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

        formatted_id = f"0{i}" if i < 10 else i
        event_id = f"event_id_{vehicle_id}_{formatted_id}"
        events_to_insert.append(
            {
                "vehicle_id": f"vehicle_{vehicle_id}",
                "event_id": event_id,
                "timestamp": event_timestamp_str,
                "event_type": random.choice(event_type_list),
                "acceleration": {
                    "x": Decimal(str(vehicle_base_data["acceleration"]["x"] + random.uniform(-0.5, 0.5))).quantize(Decimal("0.00")),
                    "y": Decimal(str(vehicle_base_data["acceleration"]["y"] + random.uniform(-0.2, 0.2))).quantize(Decimal("0.00")),
                    "z": Decimal("9.8"),
                },
                "location": {
                    "latitude": Decimal(str(vehicle_base_data["location"]["latitude"] + random.uniform(-0.01, 0.01))).quantize(Decimal("0.0")),
                    "longitude": Decimal(str(vehicle_base_data["location"]["longitude"] + random.uniform(-0.01, 0.01))).quantize(Decimal("0.00")),
                },
                "vehicle_speed": vehicle_base_data["base_speed"] + random.randint(-5, 5),
                "fuel_level": vehicle_base_data["base_fuel_level"] + random.randint(-3, 3),
                "engine_rpm": vehicle_base_data["base_rpm"] + random.randint(-200, 200),
                "throttle_position": vehicle_base_data["base_throttle_position"] + random.randint(-5, 5),
                "brake_status": random.choice(brake_status_list),
                "seatbelt_status": random.choice(seatbelt_status_list),
                "airbag_deployed": random.choice([True, False]),
                "error_codes": [f"P{random.randint(100, 999)}", f"P{random.randint(100, 999)}"],
            }
        )
        events_generated.append(event_id)

    with events.batch_writer() as batch:
        for event in events_to_insert:
            batch.put_item(Item=event)
    return events_generated


def generate_incidents(generated_event_id, incident_timestamp, num_incidents=800):
    radar_data_to_insert = []
    for i in range(num_incidents):
        radar_timestamp = incident_timestamp + datetime.timedelta(milliseconds=i)
        radar_timestamp_str = radar_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

        incident_id = f"0{i}" if i < 10 else i

        radar_data_to_insert.append(
            {
                "event_id": generated_event_id,
                "incident_id": f"incident_{incident_id}",
                "timestamp": radar_timestamp_str,
                "radar_id": f"RADAR_{i + 1}",
                "distance": Decimal(str(random.uniform(1, 100))).quantize(Decimal("0.00")),
                "velocity": Decimal(str(random.uniform(1, 50))).quantize(Decimal("0.00")),
                "azimuth_angle": random.randint(0, 360),
                "elevation_angle": random.randint(0, 90),
                "object_type": random.choice(object_type_list),
                "object_size": random.choice(object_size_list),
                "object_class": random.choice(object_class_list),
                "confidence_level": Decimal(str(random.uniform(0.5, 1.0))).quantize(Decimal("0.00")),
            }
        )

    with incidents.batch_writer() as batch:
        for radar_data in radar_data_to_insert:
            batch.put_item(Item=radar_data)


@measure_execution_time
def generate_vehicles_data(start=1, num_vehicles=100, num_incidents_per_vehicle=100, num_radar_readings_per_incident=800):
    for vehicle_id in range(start, start + num_vehicles + 1):
        vehicle_id = f"0{vehicle_id}" if vehicle_id < 10 else vehicle_id
        print(f"Generating data for Vehicle {vehicle_id}...")
        vehicle_base_data = get_vehicle_base_data()
        for incident_id in range(num_incidents_per_vehicle):
            events_generated = generate_events(vehicle_id, vehicle_base_data, num_events=random.randint(1, 5))

            incident_timestamp = datetime.datetime.now() - datetime.timedelta(seconds=incident_id * 10)
            for generated_event_id in events_generated:
                generate_incidents(generated_event_id, incident_timestamp, num_incidents=num_radar_readings_per_incident)


@measure_execution_time
def bulk_delete(vehicle_ids):
    for vehicle_id in vehicle_ids:
        events_to_delete = events.query(KeyConditionExpression=Key("vehicle_id").eq(vehicle_id))["Items"]

        # Delete incidents
        print(f"Deleting incidents for {vehicle_id}")
        for event_to_delete in events_to_delete:
            event_id = event_to_delete["event_id"]
            incidents_to_delete = incidents.query(KeyConditionExpression=Key("event_id").eq(event_id))["Items"]

            with incidents.batch_writer() as batch:
                for incident_to_delete in incidents_to_delete:
                    batch.delete_item(Key={"event_id": incident_to_delete["event_id"], "incident_id": incident_to_delete["incident_id"]})

        print(f"Deleting event for {vehicle_id}")
        with events.batch_writer() as batch:
            for event_to_delete in events_to_delete:
                batch.delete_item(Key={"vehicle_id": event_to_delete["vehicle_id"], "event_id": event_to_delete["event_id"]})


def bulk_delete_range(startId, endId):
    ids = []
    for i in range(startId, endId):
        ids.append(f"vehicle_{i}")
    bulk_delete(ids)


def get_events(vehicle_id):
    kce = Key("vehicle_id").eq(vehicle_id)
    response = events.query(KeyConditionExpression=kce)

    print(f'Found {response['Count']} events for {vehicle_id}')
    return response["Items"]


@measure_execution_time
def get_accidents(vehicle_id, accepted_obj_types, confidence_level=0.8):
    events = get_events(vehicle_id)
    total_accidents = []

    speed_change_threshold = 75

    previous_speed = None
    current_speed = None

    for event in events:
        # Filter
        kce = "event_id = :event_id"
        expression_atr_val = {":confidence_level": Decimal(str(confidence_level)), ":event_id": str(event["event_id"])}

        initial_expr = "confidence_level > :confidence_level"
        filter_expr = ""
        for object_type in accepted_obj_types:
            filter_expr = f"{filter_expr} OR contains(object_type, :{object_type})" if filter_expr else f"contains(object_type, :{object_type})"
            expression_atr_val[f":{object_type}"] = object_type

        filter_expr = f"{initial_expr} AND ({filter_expr})" if filter_expr else initial_expr

        query_params = {
            "KeyConditionExpression": kce,
            "FilterExpression": filter_expr,
            "ExpressionAttributeValues": expression_atr_val,
        }
        response = incidents.query(**query_params)
        print(f"Found {len(response["Items"])} incidents")

        # Start radar info analysis

        previous_speed = current_speed
        current_speed = event["vehicle_speed"]
        if previous_speed and current_speed:
            speed_change = abs(previous_speed - current_speed)
            speed_change_percentage = (100 * speed_change) / current_speed
            if speed_change_percentage > speed_change_threshold:
                # Check for accidents in incidents
                accident = check_if_accident(response["Items"], event)
                if accident:
                    # Add additional info
                    accident["vehicle_id"] = event["vehicle_id"]
                    total_accidents.append(accident)
                else:
                    total_accidents.append(f"False positive: Event {event["event_id"]} sudden speed change.")

    return total_accidents


def check_if_accident(event_incidents, event):
    accident_probability = 0
    if event["airbag_deployed"]:
        accident_probability += 0.3
    for incident in event_incidents:
        probability_increase = 0
        if incident["distance"] < 20:
            # Increase probability based on distance
            probability_increase = 1 - 0.2 * math.floor(incident["distance"] / 4)

        if accident_probability + probability_increase > 0.75:
            return incident

    return None


def show_accidents(accidents):
    for accident in accidents:
        if type(accident) == dict:
            print(f"""==============Found an accident==============
- IncidentId: {accident["incident_id"]}
- VehicleId: {accident["vehicle_id"]}
- EventId: {accident["event_id"]}
- Object:
  - Size: {accident["object_size"]}
  - Type: {accident["object_type"]}
  - Class: {accident["object_class"]}
  - ConfidenceLevel: {accident["confidence_level"]}""")
        else:
            print("=============================================")
            print(accident)
    print("=============================================")


if __name__ == "__main__":
    # generate_vehicles_data(10, 2, 2, 8)

    # bulk_delete(["vehicle_10", "vehicle_9"])
    bulk_delete_range(10, 13)

    # accidents = get_accidents("vehicle_01", ["vehicle", "pedestrian", "cyclist"])
    # show_accidents(accidents)
