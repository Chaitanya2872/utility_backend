// src/services/controlService.js - FIXED VERSION WITH DEBUGGING
import { publishToIoT, subscribe } from "../utils/mqttHelper.js";
import {
  getThingIdByDeviceId,
  waitForSlaveResponseFromMongoDB,
  checkBaseRespondedInMongo,
  checkTankRespondedInMongo,
  debugSlaveRequests, // NEW
} from "../services/migratedControlService.js";
import logger from "../utils/logger.js";
import { getTopic } from "../config/awsIotConfig.js";
import { User } from "../config/dbconfig.js";

/**
 * Control device via AWS Lambda (IAM-secured)
 */
export async function control(req, res) {
  logger.info("🚀 CONTROL endpoint called", {
    user: req.user?.mobile_number,
    deviceid: req.body?.deviceid,
  });

  try {
    const { deviceid } = req.body;
    if (!deviceid) {
      return res.status(400).json({ success: false, error: "Missing deviceid in request" });
    }

    const thingid = await getThingIdByDeviceId(deviceid);
    if (!thingid) {
      return res.status(404).json({ success: false, error: "DeviceId not found or no associated thing ID" });
    }

    const topic = getTopic("control", thingid, "control");
    const payload = {
      ...req.body,
      requestedBy: req.user?.mobile_number,
      timestamp: new Date().toISOString(),
    };

    await publishToIoT(topic, payload);

    logger.info("✅ Control command published via Lambda", { deviceid, thingid });

    // Remember the switch's new state. GET /status sends a poll to the
    // device's update topic carrying the stored status; without this it kept
    // sending the old "off", which flipped a switch back off right after the
    // app turned it on.
    const { switch_no, status } = req.body;
    if (switch_no && (status === "on" || status === "off")) {
      try {
        await User.updateOne(
          { "spaces.devices.device_id": deviceid },
          { $set: { "spaces.$[].devices.$[d].status": status } },
          { arrayFilters: [{ "d.device_id": deviceid, "d.switch_no": switch_no }] }
        );
      } catch (dbError) {
        logger.error(`⚠️ Could not save status for ${deviceid}/${switch_no}: ${dbError.message}`);
      }
    }

    res.status(200).json({
      success: true,
      message: "Control command published successfully",
      topic,
    });
  } catch (error) {
    logger.error("❌ Control publish error:", error);
    res.status(500).json({ success: false, error: error.message });
  }
}

/**
 * Send device settings via AWS Lambda
 */
export async function setting(deviceid, payload) {
  try {
    const thingid = await getThingIdByDeviceId(deviceid);
    if (!thingid) throw new Error(`No thingid found for deviceid: ${deviceid}`);

    const topic = getTopic("setting", thingid, "setting");
    await publishToIoT(topic, payload);

    logger.info(`✅ Settings published via Lambda for thing: ${thingid}`);
    return { success: true, topic };
  } catch (error) {
    logger.error("❌ Settings publish error:", error);
    throw new Error(`MQTT Publish Failed: ${error.message}`);
  }
}

/**
 * 🔥 FIXED: Send slave request and wait for response via MongoDB
 */
export async function slaveRequest(req, res) {
  const requestStartTime = Date.now();
  
  logger.info("🚀 SLAVE REQUEST endpoint called", {
    user: req.user?.mobile_number,
    deviceid: req.body?.deviceid,
    sensor_no: req.body?.sensor_no,
  });

  try {
    const { deviceid, sensor_no, mode, channel, addl, addh, range, capacity, slaveid } = req.body;
    
    if (!deviceid || !sensor_no) {
      return res.status(400).json({ 
        success: false, 
        error: "Missing deviceid or sensor_no" 
      });
    }

    // Get thingid
    const thingid = await getThingIdByDeviceId(deviceid);
    if (!thingid) {
      logger.error(`❌ No thingid found for deviceid: ${deviceid}`);
      return res.status(404).json({ 
        success: false, 
        error: "DeviceId not found or no associated thing ID" 
      });
    }

    logger.info(`✅ Found thingid: ${thingid} for deviceid: ${deviceid}`);

    // Build topic
    const topic = getTopic("slaveRequest", thingid, "slaveRequest");
    logger.info(`📤 Publishing to topic: ${topic}`);
    
    // Build payload exactly as device expects
    const payload = {
      deviceid,
      sensor_no,
      mode: parseInt(mode) || 3,
      channel: parseInt(channel),
      addl,
      addh,
      range: parseInt(range),
      capacity: parseInt(capacity)
    };

    // Add optional slaveid
    if (slaveid) {
      payload.slaveid = slaveid;
    }

    logger.info(`📦 Payload:`, JSON.stringify(payload, null, 2));

    // Publish via Lambda
    const publishResult = await publishToIoT(topic, payload);
    logger.info(`✅ Lambda publish result:`, publishResult);

    const publishTime = Date.now() - requestStartTime;
    logger.info(`⏱️ Publish took ${publishTime}ms`);

    // 🔍 DEBUGGING: Check what's in the database before polling
    logger.info(`🔍 Checking slave_requests collection BEFORE polling...`);
    const debugInfo = await debugSlaveRequests(thingid, deviceid);
    logger.info(`📊 Current slave_requests:`, JSON.stringify(debugInfo, null, 2));

    // Wait for response from MongoDB
    logger.info(`⏳ Waiting for response from MongoDB (thingid: ${thingid})...`);
    const response = await waitForSlaveResponseFromMongoDB(thingid, 15000); // Increased timeout

    const totalTime = Date.now() - requestStartTime;
    logger.info(`⏱️ Total request time: ${totalTime}ms`);

    if (response) {
      logger.info(`✅ RESPONSE RECEIVED:`, JSON.stringify(response, null, 2));
      
      return res.status(200).json({
        success: true,
        message: "Published and response received",
        timing: {
          publish_ms: publishTime,
          total_ms: totalTime
        },
        data: {
          status: response.response_data?.status || response.status || "success",
          deviceid: response.deviceid,
          thingid: response.thingid,
          channel: response.response_data?.channel 
            ? parseInt(response.response_data.channel) 
            : parseInt(channel),
          addl: response.response_data?.addl || addl,
          addh: response.response_data?.addh || addh,
          sensor_no: response.response_data?.sensor_no || sensor_no,
          slaveid: response.response_data?.slaveid || slaveid,
          timestamp: response.inserted_at || response.completed_at || response.requested_at
        }
      });
    } else {
      logger.warn(`⚠️ NO RESPONSE RECEIVED after ${totalTime}ms`);
      
      // 🔍 DEBUGGING: Check what's in the database after timeout
      logger.info(`🔍 Checking slave_requests collection AFTER timeout...`);
      const debugInfoAfter = await debugSlaveRequests(thingid, deviceid);
      logger.info(`📊 Current slave_requests after timeout:`, JSON.stringify(debugInfoAfter, null, 2));
      
      return res.status(200).json({
        success: true,
        message: "Published but no response received within timeout",
        timing: {
          publish_ms: publishTime,
          total_ms: totalTime
        },
        debug: {
          thingid,
          deviceid,
          sensor_no,
          topic,
          pending_requests: debugInfoAfter.pending_count,
          completed_requests: debugInfoAfter.completed_count,
          recent_requests: debugInfoAfter.recent_requests
        },
        data: null
      });
    }
  } catch (error) {
    logger.error("❌ Slave request error:", error);
    logger.error("Stack trace:", error.stack);
    
    return res.status(500).json({
      success: false,
      error: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
}

/**
 * Check if base device responded
 */
export async function isBaseResponded(req, res) {
  const { deviceid } = req.params;
  if (!deviceid) {
    return res.status(400).json({ success: false, message: "Device ID is required." });
  }

  try {
    logger.info(`Checking base response for device: ${deviceid}`);
    const responded = await checkBaseRespondedInMongo(deviceid, 5000);
    return res.status(responded ? 200 : 404).json({
      success: responded,
      message: responded ? "Base responded successfully." : "Base did not respond.",
    });
  } catch (error) {
    logger.error("Error checking base response:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error.",
      details: error.message,
    });
  }
}

/**
 * Check if tank device responded
 */
export async function isTankResponded(req, res) {
  const { deviceid, sensorNumber } = req.params;
  if (!deviceid || !sensorNumber) {
    return res.status(400).json({
      success: false,
      message: "Device ID and Sensor Number are required.",
    });
  }

  try {
    logger.info(`Checking tank response for device: ${deviceid}, sensor: ${sensorNumber}`);
    const responded = await checkTankRespondedInMongo(deviceid, sensorNumber, 10000);
    return res.status(responded ? 200 : 404).json({
      success: responded,
      message: responded ? "Tank responded successfully." : "Tank did not respond.",
    });
  } catch (error) {
    logger.error("Error checking tank response:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error.",
      details: error.message,
    });
  }
}

export default {
  control,
  setting,
  slaveRequest,
  isBaseResponded,
  isTankResponded,
};