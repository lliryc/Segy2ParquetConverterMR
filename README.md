# Segy2ParquetConverterMR

MapReduce Job for converting SEGY to Parquet format

## Overview

Segy2ParquetConverterMR is a MapReduce job designed to efficiently convert SEGY files to Parquet format. This conversion enables easier data handling, storage, and analysis in big data environments.

## Features

- **High Performance**: Utilizes the MapReduce framework for scalable and efficient processing.
- **Compatibility**: Supports SEGY-format (rev v1) files.
- **Ease of Use**: Simplifies the process of converting SEGY files to a more versatile Parquet format.

## Requirements

- Java 8 or higher
- Apache Hadoop
- Apache Maven

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/lliryc/Segy2ParquetConverterMR.git
   ```
2. Navigate to the project directory:
   ```bash
   cd Segy2ParquetConverterMR
   ```
3. Build the project using Maven:
  ```bash
  mvn clean install
  ```

## Usage

To run the conversion job, use the following command:
  ```bash
  hadoop jar target/Segy2ParquetConverterMR-1.0.jar <input_segy_path> <output_parquet_path>
  ```

## License

This project is licensed under the Apache-2.0 License.

## Contact

For any issues or questions, please create an issue in this repository.
