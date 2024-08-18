# Kafka Data Stream Processing with Python

This project demonstrates data stream processing using Apache Kafka and Python. The main tasks involve installing and configuring Kafka, testing the connection, generating and sending random measurements to a Kafka topic, and receiving and displaying these messages in a structured form.

## Project Structure

This project contains two Jupyter Notebooks:

1. **random_measurements_kafka.ipynb** - Generates and sends random measurement data (e.g., temperature, moisture, wind speed) to the Kafka topic 'measurements'.
2. **message_receiving_kafka.ipynb** - Receives and displays the messages from the 'measurements' topic.

## Data Stream Setup and Operations

1. **Kafka Installation and Configuration**
   - Install and configure Apache Kafka on your system. You can either use Docker or perform a native installation.
   - Create a Kafka topic named `measurements` using Kafka Bash commands.

2. **Kafka Python Library Installation**
   - Install the `kafka-python` library using the following command:
     ```bash
     pip install kafka-python
     ```

3. **Producer Setup (random_measurements_kafka.ipynb)**
   - Connect to the Kafka message broker.
   - Periodically generate random measurements (e.g., temperature, moisture, wind speed) and send them to the `measurements` topic.
   - The sending operation runs for 90 seconds, with messages sent every 5 seconds.

4. **Consumer Setup (message_receiving_kafka.ipynb)**
   - Connect to the Kafka message broker.
   - Receive messages from the `measurements` topic and display them in a structured format.
   - The receiving operation runs for 90 seconds, similar to the producer.

## How to Run

1. **Install Kafka and Set Up the Environment**
   - Install and configure Kafka on your local machine.
   - Create the `measurements` topic using Kafka Bash:
     ```bash
     kafka-topics.sh --create --topic measurements --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
     ```

2. **Install Required Python Packages**
   - Install the necessary Python packages by running the following command:
     ```bash
     pip install kafka-python
     ```

3. **Run the Producer Notebook**
   - Open `random_measurements_kafka.ipynb` in Jupyter Notebook.
   - Execute the cells to start sending random measurement data to the Kafka topic.

4. **Run the Consumer Notebook**
   - Open `message_receiving_kafka.ipynb` in Jupyter Notebook.
   - Execute the cells while the cells are executed in  `random_measurements_kafka.ipynb` to start receiving and displaying the messages from the Kafka topic.

## Results

- **Producer Output**: The producer notebook sends random measurements to the `measurements` topic for 90 seconds. The measurements are displayed in the terminal using the Kafka Bash consumer command.
  
- **Consumer Output**: The consumer notebook receives and displays the messages from the `measurements` topic for 90 seconds. It correctly captures and displays the measurement data in a structured format.

## Terminal Checks

- **Connection Test**: Verified using the command:
  ```bash
  kafka-console-consumer.bat --topic measurements --bootstrap-server localhost:9092 --from-beginning

## Conclusion

This project demonstrates basic data stream processing with Kafka and Python. The producer generates random measurement data, sends it to a Kafka topic, and the consumer receives and displays these messages. Kafka's ability to handle real-time streaming data is illustrated effectively, fulfilling the criteria of the Big Data Technologies portfolio exam.