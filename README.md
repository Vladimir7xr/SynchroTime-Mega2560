# SynchroTime-Mega2560

**Refactored and optimized version of the [SynchroTime](https://github.com/SergejBre/SynchroTime) project for Arduino Mega 2560 compatibility.**

## Overview
This version is a complete rewrite of the original SynchroTime sketch. While the core time-drift calculation logic remains the same, the implementation has been updated to fix stability issues on the Mega 2560 and to add hardware-level data protection.

## Major Changes and Improvements

### 1. Data Reliability (EEPROM)
*   **Triple Redundancy:** Implemented a multi-slot storage system. Persistent data is now stored in three separate locations (Primary, Backup 1, Backup 2) to prevent data loss.
*   **Integrity Checks:** Added **Checksum (CRC)** and signature validation for all settings stored in the AT24C32D EEPROM.
*   **Auto-Recovery:** The system now automatically restores valid settings from backup slots if the primary data block is corrupted.

### 3. Timing & Interrupts
*   **Safety Timeouts:** Added `SERIAL_TIMEOUT` and `BOOT_TIMEOUT` to prevent the firmware from hanging during communication or startup.

### 4. Diagnostics & Error Handling
*   **Status Indication:** Added a LED-based diagnostic system (Status/Error blinking) on Pin 13.
*   **I2C Robustness:** Added retry logic for DS3231 and EEPROM operations, replacing the original direct I2C calls with safer wrappers.

## Hardware Connection (Mega 2560)
*   **SDA/SCL:** Standard I2C pins. (SDA pin 20, SCL pin 21)
*   **SQW:** Must be connected to **Pin 2** (Required for 1Hz interrupt tracking).
*   **VCC/GND:** 5V (or better 3.3V).

## Dependencies
*   [Adafruit RTClib](https://github.com/adafruit/RTClib) (version 1.13 or higher).
*   [Wire](https://www.arduino.cc) (standard library).

## License
Modified code is released under the **MIT License**, consistent with the original project's licensing.
