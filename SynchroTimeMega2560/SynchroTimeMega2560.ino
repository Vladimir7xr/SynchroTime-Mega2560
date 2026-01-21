//------------------------------------------------------------------------------
//  SynchroTime-Mega2560
//  
//  Modified and refactored arduino sketch version of SynchroTime by SergejBre.
//  Optimized specifically for Arduino Mega 2560 compatibility.
//
//  Copyright (C) 2020 SergejBre (sergej1@email.ua)
//  Copyright (C) 2025 Vladimir7xr (https://github.com/Vladimir7xr)
//
//  Released under the MIT License.
//------------------------------------------------------------------------------
/*
This sketch performs as a server on an arduino controller for connecting the PC with an RTC DS3231 module via a serial port.
  Built-in server functions allow you to:
  - adjust the RTC DS3231 time in accordance with the reference time of your computer;
  - correct the frequency drift of the RTC DS3231 oscillator;
  - evaluate the accuracy and reliability of the RTC oscillator for a specific sample,
    as well as the chances of a successful correction in the event of a significant frequency drift;
  - save parameters and calibration data to the energy-independent flash memory AT24C32D;
  - read value from the Aging register;
  - write value to the Aging register.
The settings are:
  - Set your local time zone in the settings of the application synchroTimeApp.
    time zone = Difference of the local time and UTC-time, a value from { -12, .., -2, -1, 0, +1, +2, +3, .., +12, +13, +14 }.
  - MIN_TIME_SPAN the minimum time required for a stable calculation of the frequency drift.
Dependencies:
  - Arduino IDE version >= 1.8.13 (!Replace compilation flags from -Os to -O2);
  - Adafruit RTC library for Arduino RTClib version >= 1.13 (https://github.com/adafruit/RTClib).
Connecting DS3231 MINI module to arduino board:
  - VCC and GND of RTC DS3231 module should be connected to some power source +5V (or +3.3V)
  - SDA, SCL of RTC DS3231 module should be connected to SDA - data line, SCL - clock line of arduino
  - SQW should be connected to PIN 2 on Arduino Mega 2560
*/

#include <Wire.h>
#include <math.h>
#include "RTClib.h"

// Protocol constants
#define STARTBYTE 0x40        // Starting byte of the data set from the communication protocol

// I2C addresses
#define DS3231_ADDRESS 0x68   // I2C address for DS3231
#define EEPROM_ADDRESS 0x57   // AT24C256 address

// DS3231 register addresses
#define DS3231_AGINGREG 0x10  // Aging offset register address
#define DS3231_TEMPERATUREREG 0x11 // Temperature register
#define DS3231_STATUSREG 0x0F // Status register

// Status register bits
#define DS3231_STATUS_OSF 0x80 // Oscillator Stop Flag bit

// Timing constants
#define MIN_TIME_SPAN 100000  // Minimum time required for stable calculation of frequency drift (seconds)
#define SERIAL_TIMEOUT 1000   // Serial communication timeout in ms
#define BOOT_TIMEOUT 5000     // Timeout for serial port connection
#define I2C_DELAY 10          // Delay between I2C operations

// EEPROM constants
#define EEPROM_MIN_SIZE 4096  // Minimum EEPROM size (AT24C32)
#define EEPROM_DATA_START 0   // Start address for primary data copy
#define EEPROM_BACKUP1_START 1024 // Start address for first backup copy (¼ of min size)
#define EEPROM_BACKUP2_START 2048 // Start address for second backup copy (½ of min size)
#define EEPROM_SIGNATURE 0x55 // Signature value for EEPROM validation
#define INVALID_TIMESTAMP 0xFFFFFFFF // Invalid timestamp indicator

// Operation limits
#define I2C_MAX_ATTEMPTS 3    // Maximum number of I2C operation attempts
#define MAX_DRIFT_VALUE 1000.0f // Maximum allowed drift value in ppm
#define MIN_VALID_TIMESTAMP 1609459200 // 2021-01-01 00:00:00 UTC

// Hardware pins
#define SQW_PIN 2             // Pin for SQW interrupt (D2 on Mega 2560)
#define STATUS_LED_PIN 13     // Built-in LED pin for status indication

// Buffer sizes
#define BUFFER_SIZE 32        // Increased buffer size for safety
#define TEMP_READ_ATTEMPTS 3  // Number of temperature reading attempts

// Temperature reading constants
#define TEMP_FRACTION_MASK 0x03 // Mask for fractional part of temperature
#define TEMP_FRACTION_SHIFT 6   // Shift for fractional part of temperature

enum task_t : uint8_t { TASK_IDLE, TASK_ADJUST, TASK_INFO, TASK_CALIBR, TASK_RESET, TASK_SETREG, TASK_STATUS, TASK_WRONG };

struct time_t {
  uint32_t utc;       // UTC-time + time-zone offset
  uint16_t milliSecs;
};

// Structure for storing data in EEPROM with packed alignment
#pragma pack(push, 1)
struct PersistentData {
  uint32_t lastCalibration;
  int8_t agingOffset;
  uint8_t signature;
  uint8_t checksum;
};
#pragma pack(pop)

RTC_DS3231 rtc;
static uint8_t buff[4];
static uint8_t byteBuffer[BUFFER_SIZE];

// Global variables for time tracking with SQW
volatile uint32_t lastSQWmicros = 0;
volatile uint32_t sqwCounter = 0;
volatile bool ledState = false;  // For SQW indicator
int8_t offsetReg;
bool eepromInitialized = false;

// Function Prototypes
int8_t readFromOffsetReg(void);
bool writeToOffsetReg(const int8_t value);
static inline void memcpy_byte(void *__restrict__ dstp, const void *__restrict__ srcp, uint16_t len);
inline void intToHex(uint8_t* const buff, const uint32_t value);
inline void floatToHex(uint8_t* const buff, const float value);
uint32_t hexToInt(const uint8_t* const buff);
bool adjustTimeDrift(float drift_in_ppm);
static float calculateDrift_ppm(const time_t* const ref, const time_t* const t);
static uint8_t sumOfBytes(const uint8_t* const bbuffer, const uint8_t blength);
static float read_Temperature(void);
void resetSQWTracking(void);
bool safeAdjustRTC(uint32_t newTime);
bool safeSaveCalibrationTime(uint32_t calibrationTime);
bool rtcLostPower(void);

// EEPROM functions
uint8_t calculateChecksum(const PersistentData& data);
bool loadPersistentData(PersistentData& data);
bool savePersistentData(const PersistentData& data);
bool initializeEEPROM(void);
bool isEEPROMValid(void);
bool validatePersistentData(const PersistentData& data);

// External EEPROM functions
uint8_t i2c_eeprom_read_byte(int deviceAddress, unsigned int eeAddress);
bool i2c_eeprom_read_buffer(int deviceAddress, unsigned int eeAddress, uint8_t* buffer, int length);
bool i2c_eeprom_write_byte(int deviceAddress, unsigned int eeAddress, uint8_t data);
bool i2c_eeprom_write_page(int deviceAddress, unsigned int eeAddressPage, const uint8_t* data, uint8_t length);

// Hardware initialization functions
bool initRTC();
bool initEEPROM();
void initSQW();
void initSerial();
void initI2C();

// SQW interrupt handler
void sqwInterrupt() {
  lastSQWmicros = micros();
  sqwCounter++;
  // Toggle LED indicator (minimal code)
  ledState = !ledState;
  digitalWrite(STATUS_LED_PIN, ledState);
}

void errorBlink() {
  // Blink LED rapidly to indicate error state
  while (true) {
    digitalWrite(STATUS_LED_PIN, HIGH);
    delay(100);
    digitalWrite(STATUS_LED_PIN, LOW);
    delay(100);
  }
}

void initSerial() {
  // Initialize Serial interface with timeout
  Serial.begin(115200);
  uint32_t startTime = millis();
  while (!Serial && (millis() - startTime) < BOOT_TIMEOUT) {
    // Wait for serial port to connect with timeout
  }
}

void initI2C() {
  // Initialize I2C
  Wire.begin();
  Wire.setClock(100000);
}

bool initRTC() {
  // Initialize DS3231
  if (!rtc.begin()) {
    Serial.print(F("Couldn't find DS3231 module"));
    return false;
  }
  
  // Configure SQW output to 1Hz and set up interrupt
  rtc.writeSqwPinMode(DS3231_SquareWave1Hz);
  rtc.disable32K();  // Disable 32K output as we don't need it
  return true;
}

void initSQW() {
  pinMode(SQW_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(SQW_PIN), sqwInterrupt, FALLING);
}

bool initEEPROM() {
  // Initialize EEPROM with enhanced protection
  eepromInitialized = initializeEEPROM();
  
  // Load persistent data with recovery mechanism
  PersistentData pData;
  if (loadPersistentData(pData)) {
    offsetReg = pData.agingOffset;
    return true;
  } else {
    offsetReg = 0;
    return false;
  }
}

void setup() {
  // Initialize built-in LED for error indication
  pinMode(STATUS_LED_PIN, OUTPUT);
  digitalWrite(STATUS_LED_PIN, LOW);

  // Initialize hardware components
  initSerial();
  initI2C();
  
  if (!initRTC()) {
    errorBlink();
  }
  
  initSQW();
  
  if (!initEEPROM()) {
    Serial.print(F("EEPROM initialization failed"));
  }

  // Check if RTC lost power or EEPROM is not valid
  if (rtcLostPower() || !isEEPROMValid()) {
    // If RTC lost power or EEPROM is not valid, set to compile time
    const uint32_t newtime = DateTime(F(__DATE__), F(__TIME__)).unixtime();
    
    if (!safeAdjustRTC(newtime)) {
      Serial.print(F("Failed to adjust RTC time"));
      errorBlink();
    }
    
    // Restore aging offset from EEPROM if valid
    PersistentData pData;
    if (eepromInitialized) {
      writeToOffsetReg(offsetReg);
    } else {
      writeToOffsetReg(0);
      offsetReg = 0;
    }
    
    // Store calibration time in EEPROM with backup
    pData.lastCalibration = newtime;
    pData.agingOffset = offsetReg;
    pData.signature = EEPROM_SIGNATURE;
    pData.checksum = calculateChecksum(pData);
    if (!savePersistentData(pData)) {
      Serial.print(F("Failed to save persistent data"));
      errorBlink();
    }
    
    eepromInitialized = true;
    
    // Initialize SQW tracking
    resetSQWTracking();
  } else {
    // Initialize SQW tracking
    resetSQWTracking();
    
    // Read aging register
    int8_t currentOffset = readFromOffsetReg();
    
    // Verify aging register matches stored value
    PersistentData pData;
    if (currentOffset != offsetReg && eepromInitialized) {
      writeToOffsetReg(offsetReg);
    } else {
      offsetReg = currentOffset;
      
      // Update EEPROM if register value changed
      if (loadPersistentData(pData) && pData.agingOffset != offsetReg) {
        pData.agingOffset = offsetReg;
        pData.checksum = calculateChecksum(pData);
        if (!savePersistentData(pData)) {
          Serial.print(F("Failed to save persistent data"));
        }
      }
    }
  }
  
  Serial.print(F("Boot.. Ok, T[°C]="));
  float temperature = read_Temperature();
  Serial.print(temperature, 2);
  Serial.print(F(", Aging="));
  Serial.print(offsetReg);
  Serial.print(F(", EEPROM="));
  Serial.println(eepromInitialized ? F("Valid") : F("Invalid"));
  Serial.print(F("SQW: Enabled on pin "));
  Serial.println(SQW_PIN);
  Serial.print(F("SQW Indicator: Built-in LED (pin "));
  Serial.print(STATUS_LED_PIN);
  Serial.println(F(")"));
}

bool rtcLostPower(void) {
  // Check if RTC lost power by examining the OSF (Oscillator Stop Flag) bit
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(DS3231_ADDRESS);
    Wire.write(DS3231_STATUSREG);
    if (Wire.endTransmission() == 0) {
      delayMicroseconds(100);
      Wire.requestFrom(DS3231_ADDRESS, 1);
      if (Wire.available()) {
        uint8_t status = Wire.read();
        return (status & DS3231_STATUS_OSF) != 0; // Check OSF bit
      }
    }
    delay(I2C_DELAY);
  }
  return true; // Assume power loss if we can't read the status
}

void resetSQWTracking(void) {
  noInterrupts();
  lastSQWmicros = micros();
  sqwCounter = 0;
  interrupts();
}

bool safeAdjustRTC(uint32_t newTime) {
  rtc.adjust(DateTime(newTime));
  resetSQWTracking();
  return true;
}

bool safeSaveCalibrationTime(uint32_t calibrationTime) {
  PersistentData pData;
  if (!loadPersistentData(pData)) {
    return false;
  }
  
  // Only update if the value has changed to reduce EEPROM wear
  if (pData.lastCalibration != calibrationTime) {
    pData.lastCalibration = calibrationTime;
    pData.checksum = calculateChecksum(pData);
    
    if (!savePersistentData(pData)) {
      return false;
    }
  }
  
  return true;
}

// Function to get current time with millisecond precision using SQW
time_t getTime() {
  time_t result;
  static uint32_t lastProcessedSQW = 0;
  static uint32_t lastProcessedMicros = 0;
  
  // Read RTC time
  DateTime now = rtc.now();
  result.utc = now.unixtime();
  
  // Atomically read SQW variables
  uint32_t currentSQW;
  uint32_t currentLastSQWmicros;
  noInterrupts();
  currentSQW = sqwCounter;
  currentLastSQWmicros = lastSQWmicros;
  interrupts();
  
  // If we have a new SQW pulse since last processing
  if (currentSQW != lastProcessedSQW) {
    lastProcessedSQW = currentSQW;
    lastProcessedMicros = currentLastSQWmicros;
    result.milliSecs = 0; // Beginning of a new second
  } else {
    // Calculate milliseconds since last SQW pulse
    uint32_t currentMicros = micros();
    uint32_t elapsedMicros;
    
    // Handle micros() overflow
    if (currentMicros >= lastProcessedMicros) {
      elapsedMicros = currentMicros - lastProcessedMicros;
    } else {
      elapsedMicros = (0xFFFFFFFF - lastProcessedMicros) + currentMicros;
    }
    
    result.milliSecs = elapsedMicros / 1000;
    
    // Handle case where we're near the end of a second
    if (result.milliSecs >= 1000) {
      result.milliSecs = 999; // Limit to max 999 ms
    }
  }
  
  return result;
}

void loop() {
  task_t task = TASK_IDLE;
  bool ok = false;
  uint8_t byteCounter = 0U;
  uint8_t numberOfBytes = 0U;
  float drift_in_ppm = .0f;
  time_t t, ref;
  PersistentData pData;

  // Get current time with millisecond precision
  t = getTime();

  // Reset buffer at the beginning of each loop iteration
  memset(byteBuffer, 0, BUFFER_SIZE);
  byteCounter = 0U;

  if (Serial.available() > 1) {
    // Check for start byte with timeout
    uint32_t startTime = millis();
    while (Serial.available() < 2 && (millis() - startTime) < SERIAL_TIMEOUT) {
      delay(1);
    }
    
    if (Serial.available() < 2) {
      // Timeout or not enough data
      return;
    }
    
    if (Serial.read() == STARTBYTE) {
      // Command Parser
      char thisChar = Serial.read();
      uint8_t expectedBytes = 0;
      
      switch (thisChar) {
        case 'a': // time adjustment request
          expectedBytes = 7;
          task = TASK_ADJUST;
          break;
        case 'i': // information request
          expectedBytes = 7;
          task = TASK_INFO;
          break;
        case 'c': // calibrating request
          expectedBytes = 7;
          task = TASK_CALIBR;
          break;
        case 'r': // reset request
          expectedBytes = 1;
          task = TASK_RESET;
          break;
        case 's': // set offset reg. request
          expectedBytes = 5;
          task = TASK_SETREG;
          break;
        case 't': // status request
          expectedBytes = 1;
          task = TASK_STATUS;
          break;
        default:  // unknown request
          task = TASK_IDLE;
          Serial.print(F("Unknown Request "));
          Serial.print(thisChar);
      }

      if (task != TASK_IDLE) {
        // Read data with timeout and buffer protection
        startTime = millis();
        while (Serial.available() < expectedBytes && (millis() - startTime) < SERIAL_TIMEOUT) {
          delay(1);
        }
        
        if (Serial.available() < expectedBytes) {
          // Not enough data received
          task = TASK_IDLE;
          Serial.print(F("Timeout waiting for data"));
        } else {
          // Read exactly the expected number of bytes
          numberOfBytes = Serial.readBytes(byteBuffer, min(expectedBytes, BUFFER_SIZE));
        }
      }

      // Data Parser - added size check
      uint8_t crc = 0U;
      uint8_t sum = uint8_t(thisChar);
      if (numberOfBytes > 0 && numberOfBytes < BUFFER_SIZE) {
        if (numberOfBytes > sizeof(ref)) {
          if (numberOfBytes >= sizeof(ref) + 1) {
            memcpy_byte(&ref, byteBuffer, sizeof(ref));
            crc = byteBuffer[sizeof(ref)];
            sum += sumOfBytes(byteBuffer, sizeof(ref));
          }
        } else if (numberOfBytes > sizeof(drift_in_ppm)) {
          if (numberOfBytes >= sizeof(drift_in_ppm) + 1) {
            memcpy_byte(&drift_in_ppm, byteBuffer, sizeof(drift_in_ppm));
            crc = byteBuffer[sizeof(drift_in_ppm)];
            sum += sumOfBytes(byteBuffer, sizeof(drift_in_ppm));
          }
        } else {
          crc = byteBuffer[0];
        }

        // checksum verification
        if (crc != sum) {
          task = TASK_IDLE;
          Serial.print(F("Invalid Data"));
        } else if (task != TASK_IDLE) {
          // Added buffer overflow check before writing STARTBYTE
          if (byteCounter < BUFFER_SIZE) {
            byteBuffer[byteCounter] = STARTBYTE;
            byteCounter++;
          } else {
            task = TASK_IDLE;
            Serial.print(F("Buffer overflow"));
          }
        }
      } else {
        task = TASK_IDLE;
        Serial.print(F("Invalid data size"));
      }
    }
  }

  // Helper function for safe data addition to buffer
  auto safeAddToBuffer = [&](const void* data, size_t dataSize) -> bool {
    if (byteCounter + dataSize >= BUFFER_SIZE) {
      Serial.print(F("Buffer overflow prevented"));
      return false;
    }
    memcpy_byte(byteBuffer + byteCounter, data, dataSize);
    byteCounter += dataSize;
    return true;
  };

  // Helper function for safe byte addition
  auto safeAddByte = [&](uint8_t byte) -> bool {
    if (byteCounter + 1 >= BUFFER_SIZE) {
      Serial.print(F("Buffer overflow prevented"));
      return false;
    }
    byteBuffer[byteCounter] = byte;
    byteCounter++;
    return true;
  };

  switch (task) {
    case TASK_ADJUST: // adjust time
      if (ref.utc > MIN_VALID_TIMESTAMP) {
        ok = safeAdjustRTC(ref.utc);
        
        // Store calibration time with enhanced protection
        if (ok) {
          ok = safeSaveCalibrationTime(ref.utc);
        }
      }
      safeAddByte(ok);
      break;
      
    case TASK_INFO: // information
      if (!safeAddToBuffer(&t, sizeof(t))) break;
      
      if (!safeAddByte(readFromOffsetReg())) break;
      
      drift_in_ppm = calculateDrift_ppm(&ref, &t);
      if (!safeAddToBuffer(&drift_in_ppm, sizeof(drift_in_ppm))) break;
      
      // Add calibration time to response
      if (loadPersistentData(pData)) {
        if (!safeAddToBuffer(&pData.lastCalibration, sizeof(pData.lastCalibration))) break;
      }
      break;
      
    case TASK_CALIBR: // calibrating
      if (!safeAddByte(readFromOffsetReg())) break;
      
      drift_in_ppm = calculateDrift_ppm(&ref, &t);
      if (isnan(drift_in_ppm) || isinf(drift_in_ppm) || fabsf(drift_in_ppm) < 0.1) {
        // Drift is invalid or too small to calibrate
        ok = true;
      } else {
        ok = adjustTimeDrift(drift_in_ppm);
      }
      
      if (!safeAddToBuffer(&drift_in_ppm, sizeof(drift_in_ppm))) break;
      
      if (ok) {
        if (ref.utc > MIN_VALID_TIMESTAMP) {
          ok = safeAdjustRTC(ref.utc);
          
          // Store calibration time with enhanced protection
          if (ok) {
            ok = safeSaveCalibrationTime(ref.utc);
          }
        }
        
        if (ok) {
          if (!safeAddByte(readFromOffsetReg())) break;
        }
      }
      
      if (!safeAddByte(ok)) break;
      break;
      
    case TASK_RESET: // reset
      ok = writeToOffsetReg(0);
      
      // Reset persistent data with enhanced protection
      pData.lastCalibration = INVALID_TIMESTAMP;
      pData.agingOffset = 0;
      pData.signature = EEPROM_SIGNATURE;
      pData.checksum = calculateChecksum(pData);
      ok = savePersistentData(pData) && ok;
      
      resetSQWTracking();
      
      if (!safeAddByte(ok)) break;
      break;
      
    case TASK_SETREG: // set register
      ok = adjustTimeDrift(drift_in_ppm);
      if (!safeAddByte(ok)) break;
      break;
      
    case TASK_STATUS: // get status
      if (!safeAddByte(eepromInitialized ? 0x01 : 0x00)) break;
      break;
      
    case TASK_IDLE: // idle task
      break;
      
    default:
      Serial.print(F("Unknown Task "));
      Serial.print(task, HEX);
  }
  
  // Response to the request - added overflow check
  if (byteCounter > 0 && byteCounter < BUFFER_SIZE - 1) {
    byteBuffer[byteCounter] = sumOfBytes(byteBuffer, byteCounter);
    Serial.write(byteBuffer, ++byteCounter);
    Serial.flush();
  } else if (byteCounter >= BUFFER_SIZE - 1) {
    Serial.print(F("Response too large, truncating"));
    // Add checksum to the last possible byte
    byteBuffer[BUFFER_SIZE - 2] = sumOfBytes(byteBuffer, BUFFER_SIZE - 2);
    Serial.write(byteBuffer, BUFFER_SIZE - 1);
    Serial.flush();
  }
}

int8_t readFromOffsetReg(void) {
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(DS3231_ADDRESS);
    Wire.write(uint8_t(DS3231_AGINGREG));
    if (Wire.endTransmission() == 0) {
      delayMicroseconds(100); // Short delay for register access
      Wire.requestFrom(uint8_t(DS3231_ADDRESS), uint8_t(1));
      if (Wire.available() == 1) {
        return int8_t(Wire.read());
      }
    }
    delay(I2C_DELAY); // Delay before retry
  }
  return 0; // Return 0 after failed attempts
}

bool writeToOffsetReg(const int8_t value) {
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(DS3231_ADDRESS);
    Wire.write(uint8_t(DS3231_AGINGREG));
    Wire.write(value);
    if (Wire.endTransmission() == 0) {
      return true;
    }
    delay(I2C_DELAY); // Delay before retry
  }
  return false;
}

inline void intToHex(uint8_t* const buff, const uint32_t value) {
  memcpy_byte(buff, &value, sizeof(value));
}

inline void floatToHex(uint8_t* const buff, const float value) {
  memcpy_byte(buff, &value, sizeof(value));
}

uint32_t hexToInt(const uint8_t* const buff) {
  uint32_t result;
  memcpy_byte(&result, buff, sizeof(result));
  return result;
}

bool adjustTimeDrift(float drift_in_ppm) {
  // Check for NaN, infinity and extreme values
  if (isnan(drift_in_ppm) || isinf(drift_in_ppm) || fabsf(drift_in_ppm) > MAX_DRIFT_VALUE) return false;
  
  // Load current data
  PersistentData pData;
  if (!loadPersistentData(pData) || !validatePersistentData(pData)) return false;
  
  // Calculate new offset using standard roundf function
  int32_t offset = (int32_t)roundf(drift_in_ppm * 10);
  offset += pData.agingOffset;
  
  // Check for integer overflow with saturation
  if (offset > 127) {
    offset = 127;
  } else if (offset < -128) {
    offset = -128;
  }
  
  // Check if offset needs to be changed
  if (offset == pData.agingOffset) {
    return true; // No change needed
  }
  
  // Save to both register and EEPROM
  bool ok = writeToOffsetReg(offset);
  if (ok) {
    pData.agingOffset = offset;
    pData.checksum = calculateChecksum(pData);
    ok = savePersistentData(pData);
    if (ok) {
      offsetReg = offset; // Update global variable
    }
  }
  
  return ok;
}

static float calculateDrift_ppm(const time_t* const referenceTime, const time_t* const clockTime) {
  // Load persistent data
  PersistentData pData;
  if (!loadPersistentData(pData) || !validatePersistentData(pData) || pData.lastCalibration == INVALID_TIMESTAMP) {
    return 0;
  }
  
  const uint32_t last_set_timeUTC = pData.lastCalibration;
  const int32_t diff = referenceTime->utc - last_set_timeUTC;
  
  // Check for minimum time span and valid difference
  if (diff <= 0 || diff < MIN_TIME_SPAN) {
    return 0;
  }
  
  const int32_t time_driftSecs = clockTime->utc - referenceTime->utc;
  const int16_t time_driftMs = clockTime->milliSecs - referenceTime->milliSecs;
  
  // Optimized calculation using direct division
  float result = (time_driftSecs * 1000000.0f + time_driftMs * 1000.0f) / diff;
  
  // Check for extreme values that might indicate calculation errors
  if (isnan(result) || isinf(result) || fabsf(result) > MAX_DRIFT_VALUE) {
    return 0;
  }
  
  return result;
}

static uint8_t sumOfBytes(const uint8_t* const bbuffer, const uint8_t blength) {
  uint8_t sum = 0U;
  for (uint8_t idx = 0U; idx < blength; idx++) {
    sum += bbuffer[idx];
  }
  return sum;
}

uint8_t calculateChecksum(const PersistentData& data) {
  const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&data);
  uint8_t sum = 0;
  for (size_t i = 0; i < sizeof(PersistentData) - sizeof(data.checksum); i++) {
    sum += bytes[i];
  }
  return sum;
}

bool loadPersistentData(PersistentData& data) {
  PersistentData copies[3];
  bool valid[3] = {false, false, false};
  
  // Read and validate all three copies
  valid[0] = i2c_eeprom_read_buffer(EEPROM_ADDRESS, EEPROM_DATA_START, 
             reinterpret_cast<uint8_t*>(&copies[0]), sizeof(PersistentData)) &&
             validatePersistentData(copies[0]);
  
  valid[1] = i2c_eeprom_read_buffer(EEPROM_ADDRESS, EEPROM_BACKUP1_START, 
             reinterpret_cast<uint8_t*>(&copies[1]), sizeof(PersistentData)) &&
             validatePersistentData(copies[1]);
  
  valid[2] = i2c_eeprom_read_buffer(EEPROM_ADDRESS, EEPROM_BACKUP2_START, 
             reinterpret_cast<uint8_t*>(&copies[2]), sizeof(PersistentData)) &&
             validatePersistentData(copies[2]);
  
  // Apply majority vote algorithm
  if (valid[0] && valid[1] && memcmp(&copies[0], &copies[1], sizeof(PersistentData)) == 0) {
    data = copies[0];
    if (!valid[2]) {
      // Restore third copy
      i2c_eeprom_write_page(EEPROM_ADDRESS, EEPROM_BACKUP2_START, 
                           reinterpret_cast<const uint8_t*>(&data), sizeof(PersistentData));
    }
    return true;
  }
  
  if (valid[0] && valid[2] && memcmp(&copies[0], &copies[2], sizeof(PersistentData)) == 0) {
    data = copies[0];
    if (!valid[1]) {
      // Restore second copy
      i2c_eeprom_write_page(EEPROM_ADDRESS, EEPROM_BACKUP1_START, 
                           reinterpret_cast<const uint8_t*>(&data), sizeof(PersistentData));
    }
    return true;
  }
  
  if (valid[1] && valid[2] && memcmp(&copies[1], &copies[2], sizeof(PersistentData)) == 0) {
    data = copies[1];
    if (!valid[0]) {
      // Restore primary copy
      i2c_eeprom_write_page(EEPROM_ADDRESS, EEPROM_DATA_START, 
                           reinterpret_cast<const uint8_t*>(&data), sizeof(PersistentData));
    }
    return true;
  }
  
  // If we get here, there are no two identical valid copies
  // Try to use any valid copy
  for (int i = 0; i < 3; i++) {
    if (valid[i]) {
      data = copies[i];
      // Restore other copies
      for (int j = 0; j < 3; j++) {
        if (j != i) {
          uint16_t address = (j == 0) ? EEPROM_DATA_START : 
                            ((j == 1) ? EEPROM_BACKUP1_START : EEPROM_BACKUP2_START);
          i2c_eeprom_write_page(EEPROM_ADDRESS, address, 
                               reinterpret_cast<const uint8_t*>(&data), sizeof(PersistentData));
        }
      }
      return true;
    }
  }
  
  return false; // All copies are invalid
}

bool savePersistentData(const PersistentData& data) {
  // Correct write order: start with the most distant copy
  const uint16_t addresses[] = {
    EEPROM_BACKUP2_START,  // First write to the most distant copy
    EEPROM_BACKUP1_START,  // Then to the first backup
    EEPROM_DATA_START      // And only then to the primary copy
  };

  // Write all copies in the correct order
  for (int i = 0; i < 3; i++) {
    if (!i2c_eeprom_write_page(EEPROM_ADDRESS, addresses[i], 
         reinterpret_cast<const uint8_t*>(&data), sizeof(PersistentData))) {
      return false;
    }
    
    // Verify the write operation
    delay(5); // Give time for EEPROM write completion
    PersistentData verifyData;
    if (!i2c_eeprom_read_buffer(EEPROM_ADDRESS, addresses[i], 
         reinterpret_cast<uint8_t*>(&verifyData), sizeof(PersistentData)) ||
        memcmp(&data, &verifyData, sizeof(PersistentData)) != 0) {
      return false;
    }
  }
  
  return true;
}

bool validatePersistentData(const PersistentData& data) {
  return data.checksum == calculateChecksum(data) && data.signature == EEPROM_SIGNATURE;
}

bool initializeEEPROM(void) {
  PersistentData pData;
  
  // Try to load data
  if (loadPersistentData(pData)) {
    return true; // Data successfully loaded
  }
  
  // If unable to load, initialize with default values
  pData.lastCalibration = INVALID_TIMESTAMP;
  pData.agingOffset = 0;
  pData.signature = EEPROM_SIGNATURE;
  pData.checksum = calculateChecksum(pData);
  
  // Save data to all three copies
  return savePersistentData(pData);
}

bool isEEPROMValid(void) {
  PersistentData pData;
  return loadPersistentData(pData) && validatePersistentData(pData) && pData.lastCalibration != INVALID_TIMESTAMP;
}

static inline void memcpy_byte(void *__restrict__ dstp, const void *__restrict__ srcp, uint16_t len) {
  uint8_t *dst = (uint8_t*) dstp;
  const uint8_t *src = (const uint8_t*) srcp;
  for (uint16_t idx = 0U; idx < len; idx++) {
    *(dst++) = *(src++);
  }
}

// Corrected temperature reading function
static float read_Temperature() {
  for (uint8_t attempt = 0; attempt < TEMP_READ_ATTEMPTS; attempt++) {
    Wire.beginTransmission(DS3231_ADDRESS);
    Wire.write(DS3231_TEMPERATUREREG);
    if (Wire.endTransmission() != 0) {
      delay(I2C_DELAY);
      continue;
    }
    
    delayMicroseconds(100); // Short delay for register access
    
    Wire.requestFrom(DS3231_ADDRESS, 2);
    if (Wire.available() < 2) {
      delay(I2C_DELAY);
      continue;
    }
    
    int8_t whole = Wire.read(); // Integer part (signed)
    uint8_t fraction = Wire.read(); // Fractional part
    
    // Extract fractional part (upper 2 bits)
    fraction = (fraction >> TEMP_FRACTION_SHIFT) & TEMP_FRACTION_MASK; // Now fraction is 0, 1, 2, or 3
    
    // Calculate temperature
    float temperature = whole + fraction * 0.25f;
    
    return temperature;
  }
  return 0.0; // Return 0 after failed attempts
}

// External EEPROM functions
uint8_t i2c_eeprom_read_byte(int deviceAddress, unsigned int eeAddress) {
  uint8_t rdata = 0xFF;
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(deviceAddress);
    Wire.write((int)(eeAddress >> 8)); // MSB
    Wire.write((int)(eeAddress & 0xFF)); // LSB
    if (Wire.endTransmission() == 0) {
      Wire.requestFrom(deviceAddress, 1);
      if (Wire.available()) {
        rdata = Wire.read();
        break;
      }
    }
    delay(I2C_DELAY);
  }
  return rdata;
}

bool i2c_eeprom_read_buffer(int deviceAddress, unsigned int eeAddress, uint8_t* buffer, int length) {
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(deviceAddress);
    Wire.write((int)(eeAddress >> 8)); // MSB
    Wire.write((int)(eeAddress & 0xFF)); // LSB
    if (Wire.endTransmission() == 0) {
      Wire.requestFrom(deviceAddress, length);
      for (int i = 0; i < length; i++) {
        if (Wire.available()) {
          buffer[i] = Wire.read();
        } else {
          return false;
        }
      }
      return true;
    }
    delay(I2C_DELAY);
  }
  return false;
}

bool i2c_eeprom_write_byte(int deviceAddress, unsigned int eeAddress, uint8_t data) {
  // Check if we really need to write to reduce EEPROM wear
  uint8_t currentData = i2c_eeprom_read_byte(deviceAddress, eeAddress);
  if (currentData == data) {
    return true; // Data is already the same, no need to write
  }
  
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(deviceAddress);
    Wire.write((int)(eeAddress >> 8)); // MSB
    Wire.write((int)(eeAddress & 0xFF)); // LSB
    Wire.write(data);
    if (Wire.endTransmission() == 0) {
      return true;
    }
    delay(I2C_DELAY);
  }
  return false;
}

bool i2c_eeprom_write_page(int deviceAddress, unsigned int eeAddressPage, const uint8_t* data, uint8_t length) {
  for (uint8_t attempt = 0; attempt < I2C_MAX_ATTEMPTS; attempt++) {
    Wire.beginTransmission(deviceAddress);
    Wire.write((int)(eeAddressPage >> 8)); // MSB
    Wire.write((int)(eeAddressPage & 0xFF)); // LSB
    for (uint8_t i = 0; i < length; i++) {
      Wire.write(data[i]);
    }
    if (Wire.endTransmission() == 0) {
      return true;
    }
    delay(I2C_DELAY);
  }
  return false;
}



