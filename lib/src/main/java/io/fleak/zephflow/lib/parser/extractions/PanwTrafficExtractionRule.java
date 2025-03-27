/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.parser.extractions;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** Created by bolei on 2/26/25 */
public class PanwTrafficExtractionRule implements ExtractionRule {
  private static final Pattern CSV_SPLIT_PATTERN =
      Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

  // TODO: This implementation only works for PAN-OS 10.1. We need to add support for other versions
  // Define field names according to PAN-OS traffic log specification
  // Reference:
  // https://docs.paloaltonetworks.com/pan-os/10-1/pan-os-admin/monitoring/use-syslog-for-monitoring/syslog-field-descriptions/traffic-log-fields
  private static final String[] LOG_FIELDS = {
    "FUTURE_USE",
    "receiveTime",
    "serialNumber",
    "type",
    "subtype",
    "FUTURE_USE",
    "generatedTime",
    "sourceAddress",
    "destinationAddress",
    "natSourceIP",
    "natDestinationIP",
    "ruleName",
    "sourceUser",
    "destinationUser",
    "application",
    "virtualSystem",
    "sourceZone",
    "destinationZone",
    "inboundInterface",
    "outboundInterface",
    "logAction",
    "FUTURE_USE",
    "sessionID",
    "repeatCount",
    "sourcePort",
    "destinationPort",
    "natSourcePort",
    "natDestinationPort",
    "flags",
    "protocol",
    "action",
    "bytes",
    "bytesSent",
    "bytesReceived",
    "packets",
    "startTime",
    "elapsedTime",
    "category",
    "FUTURE_USE",
    "sequenceNumber",
    "actionFlags",
    "sourceCountry",
    "destinationCountry",
    "FUTURE_USE",
    "packetsSent",
    "packetsReceived",
    "sessionEndReason",
    "deviceGroupHierarchyLevel1",
    "deviceGroupHierarchyLevel2",
    "deviceGroupHierarchyLevel3",
    "deviceGroupHierarchyLevel4",
    "virtualSystemName",
    "deviceName",
    "actionSource",
    "sourceVMUUID",
    "destinationVMUUID",
    "tunnelID_IMSI",
    "monitorTag_IMEI",
    "parentSessionID",
    "parentStartTime",
    "tunnelType",
    "sctpAssociationID",
    "sctpChunks",
    "sctpChunksSent",
    "sctpChunksReceived",
    "ruleUUID",
    "http2Connection",
    "appFlapCount",
    "policyID",
    "linkSwitches",
    "sdwanCluster",
    "sdwanDeviceType",
    "sdwanClusterType",
    "sdwanSite",
    "dynamicUserGroupName",
    "xffAddress",
    "sourceDeviceCategory",
    "sourceDeviceProfile",
    "sourceDeviceModel",
    "sourceDeviceVendor",
    "sourceDeviceOSFamily",
    "sourceDeviceOSVersion",
    "sourceHostname",
    "sourceMacAddress",
    "destinationDeviceCategory",
    "destinationDeviceProfile",
    "destinationDeviceModel",
    "destinationDeviceVendor",
    "destinationDeviceOSFamily",
    "destinationDeviceOSVersion",
    "destinationHostname",
    "destinationMacAddress",
    "containerID",
    "podNamespace",
    "podName",
    "sourceExternalDynamicList",
    "destinationExternalDynamicList",
    "hostID",
    "userDeviceSerialNumber",
    "sourceDynamicAddressGroup",
    "destinationDynamicAddressGroup",
    "sessionOwner",
    "highResolutionTimestamp",
    "aSliceServiceType",
    "aSliceDifferentiator",
    "applicationSubcategory",
    "applicationCategory",
    "applicationTechnology",
    "applicationRisk",
    "applicationCharacteristic",
    "applicationContainer",
    "tunneledApplication",
    "applicationSaaS",
    "applicationSanctionedState",
    "offloaded"
  };

  @Override
  public RecordFleakData extract(String logEntry) throws Exception {
    Map<String, String> parsedLog = new HashMap<>();
    String[] values = splitCSVLine(logEntry);
    for (int i = 0; i < Math.min(values.length, LOG_FIELDS.length); i++) {
      if (!LOG_FIELDS[i].equals("FUTURE_USE")) {
        parsedLog.put(LOG_FIELDS[i], values[i]);
      }
    }
    // Handle potential metadata appended at the end with $$ delimiter
    if (logEntry.contains(" $$ ")) {
      String metadataPart = logEntry.substring(logEntry.indexOf(" $$ ") + 4);
      String[] metadataItems = metadataPart.split(" ");

      for (String item : metadataItems) {
        if (item.contains("=")) {
          String[] keyValue = item.split("=", 2);
          parsedLog.put("metadata_" + keyValue[0], keyValue[1]);
        }
      }
    }

    return (RecordFleakData) FleakData.wrap(parsedLog);
  }

  private static String[] splitCSVLine(String line) {
    // Remove any trailing metadata part with $$ delimiter
    if (line.contains(" $$ ")) {
      line = line.substring(0, line.indexOf(" $$ "));
    }

    // Pattern for CSV parsing that handles quoted fields

    String[] fields = CSV_SPLIT_PATTERN.split(line);

    // Remove quotes from quoted fields
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].startsWith("\"") && fields[i].endsWith("\"")) {
        fields[i] = fields[i].substring(1, fields[i].length() - 1);
      }
    }

    return fields;
  }
}
