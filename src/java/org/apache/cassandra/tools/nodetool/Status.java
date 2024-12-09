/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.PrintStream;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.Collections;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import com.google.common.collect.ArrayListMultimap;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
@Command(name = "status", description = "Print cluster information (state, load, IDs, ...)")
public class Status extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace>]", description = "The keyspace name")
    private String keyspace = null;

    @Option(title = "resolve_ip", name = {"-r", "--resolve-ip"}, description = "Show node domain names instead of IPs")
    private boolean resolveIp = false;

    @Option(title = "sort", name = {"-s", "--sort"}, description = "Sort by [IP, Load, Owns, ID, Rack, Status]")
    private String sortBy = null;


    @Option(title = "sort_order", name = {"-o","--order"}, description = "Sort order: a or ascending for ascending, d or descending for descending")
    private String sortOrder = null;

    private boolean isTokenPerNode = true;
    private Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
    private Map<String, String> loadMap, hostIDMap;
    private EndpointSnitchInfoMBean epSnitchInfo;
    SortedMap<String, List<Object>> data;
    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        joiningNodes = probe.getJoiningNodes(true);
        leavingNodes = probe.getLeavingNodes(true);
        movingNodes = probe.getMovingNodes(true);
        loadMap = probe.getLoadMap(true);
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap(true);
        liveNodes = probe.getLiveNodes(true);
        unreachableNodes = probe.getUnreachableNodes(true);
        hostIDMap = probe.getHostIdMap(true);
        epSnitchInfo = probe.getEndpointSnitchInfoProxy();

        StringBuilder errors = new StringBuilder();
        TableBuilder.SharedTable sharedTable = new TableBuilder.SharedTable("  ");

        Map<String, Float> ownerships = null;
        boolean hasEffectiveOwns = false;
        try
        {
            ownerships = probe.effectiveOwnershipWithPort(keyspace);
            hasEffectiveOwns = true;
        }
        catch (IllegalStateException e)
        {
            try
            {
                ownerships = probe.getOwnershipWithPort();
                errors.append("Note: ").append(e.getMessage()).append("%n");
            }
            catch (Exception ex)
            {
                out.printf("%nError: %s%n", ex.getMessage());
                System.exit(1);
            }
        }
        catch (IllegalArgumentException ex)
        {
            out.printf("%nError: %s%n", ex.getMessage());
            System.exit(1);
        }
        try
        {
            if (sortOrder != null && sortBy == null) {
                throw new IllegalArgumentException("Sort order (a or ascending or d or descending) can only be used with -s or --sort.");
            }
            if(getSortOrder()==3)
            {
                throw new IllegalArgumentException("Given Wrong Value for Sort order (a or ascending or d or descending)");
            }
        }
        catch (IllegalArgumentException ex)
        {
            out.printf("%nError: %s%n", ex.getMessage());
            System.exit(1);
        }

        SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

        // More tokens than nodes (aka vnodes)?
        if (dcs.size() < tokensToEndpoints.size())
            isTokenPerNode = false;

        // Datacenters
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            data = new TreeMap<>();
            TableBuilder tableBuilder = sharedTable.next();
            addNodesHeader(hasEffectiveOwns, tableBuilder);

            ArrayListMultimap<String, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
            for (HostStatWithPort stat : dc.getValue())
                hostToTokens.put(stat.endpointWithPort.getHostAddressAndPort(), stat);

            for (String endpoint : hostToTokens.keySet())
            {
                Float owns = ownerships.get(endpoint);
                List<HostStatWithPort> tokens = hostToTokens.get(endpoint);
                addNode(endpoint, owns, tokens.get(0), tokens.size(), hasEffectiveOwns, tableBuilder);
            }
            if(sortBy!=null)
            {
                sortData(tableBuilder,probe);
            }
        }

        Iterator<TableBuilder> results = sharedTable.complete().iterator();
        boolean first = true;
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            if (!first) {
                out.println();
            }
            first = false;
            String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
            out.print(dcHeader);
            for (int i = 0; i < (dcHeader.length() - 1); i++) out.print('=');
            out.println();

            // Legend
            out.println("Status=Up/Down");
            out.println("|/ State=Normal/Leaving/Joining/Moving");
            TableBuilder dcTable = results.next();
            dcTable.printTo(out);
        }

        out.printf("%n" + errors);
    }

    private void addNodesHeader(boolean hasEffectiveOwns, TableBuilder tableBuilder)
    {
        String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

        if (isTokenPerNode)
            tableBuilder.add("--", "Address", "Load", owns, "Host ID", "Token", "Rack");
        else
            tableBuilder.add("--", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
    }

    private void addNode(String endpoint, Float owns, HostStatWithPort hostStat, int size, boolean hasEffectiveOwns,
                         TableBuilder tableBuilder)
    {
        String status, state, load, strOwns, hostID, rack, epDns;
        if (liveNodes.contains(endpoint)) status = "U";
        else if (unreachableNodes.contains(endpoint)) status = "D";
        else status = "?";
        if (joiningNodes.contains(endpoint)) state = "J";
        else if (leavingNodes.contains(endpoint)) state = "L";
        else if (movingNodes.contains(endpoint)) state = "M";
        else state = "N";

        String statusAndState = status.concat(state);
        load = loadMap.getOrDefault(endpoint, "?");
        strOwns = owns != null && hasEffectiveOwns ? new DecimalFormat("##0.0%").format(owns) : "?";
        hostID = hostIDMap.get(endpoint);

        try
        {
            rack = epSnitchInfo.getRack(endpoint);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        epDns = hostStat.ipOrDns(printPort);
        if (sortBy != null)
        {
            if (isTokenPerNode)
            {
                data.put(epDns,new ArrayList<>(Arrays.asList(statusAndState, load, strOwns, hostID, hostStat.token, rack)));
            }
            else
            {
                data.put(epDns,new ArrayList<>(Arrays.asList(statusAndState, load, String.valueOf(size), strOwns, hostID, rack)));
            }
        }
        else
        {
            if (isTokenPerNode)
            {
                tableBuilder.add(statusAndState, epDns, load, strOwns, hostID, hostStat.token, rack);
            }
            else
            {
                tableBuilder.add(statusAndState, epDns, load, String.valueOf(size), strOwns, hostID, rack);
            }
        }
    }


    // Method to sort data based on specified criteria and print it in the sorted order
    private void sortData(TableBuilder tableBuilder, NodeProbe probe) {
        PrintStream out = probe.output().out;

        // Convert the map entries to a list for sorting
        List<Map.Entry<String, List<Object>>> entryList = new ArrayList<>(data.entrySet());

        // Sort data based on the specified sorting criteria
        switch (sortBy) {
            case "ip":
                sortByIp(entryList, false); // Sort by IP address
                break;
            case "load":
                sortByNumericIndex(entryList, 1, true); // Sort by load, treated as numeric
                break;
            case "id":
                // Sort by ID, with logic varying based on whether tokens per node is used
                sortByIndex(entryList, isTokenPerNode ? 3 : 4, false);
                break;
            case "rack":
                sortByIndex(entryList, 5, false); // Sort by rack information
                break;
            case "owns":
                // Sort by ownership percentage, with different indices based on token settings
                sortByIndex(entryList, isTokenPerNode ? 2 : 3, true);
                break;
            case "status":
                sortByIndex(entryList, 0, true); // Sort by status
                break;
            default:
                // Handle invalid sorting criteria with an error message and exit
                out.printf("%nError: Given Wrong Value for -s or --sort(ip,load,id,rack,owns,status)%n");
                System.exit(1);
        }

        // Print the sorted data
        sortPrinter(entryList, tableBuilder);
    }

    // Method to sort entries by IP address, optionally descending
    private void sortByIp(List<Map.Entry<String, List<Object>>> entryList, boolean descending) {
        entryList.sort((entry1, entry2) -> {
            String key1 = entry1.getKey();
            String key2 = entry2.getKey();

            // Check if the keys are valid IP addresses
            boolean isIp1 = key1.matches("\\d+\\.\\d+\\.\\d+\\.\\d+(:\\d+)?");
            boolean isIp2 = key2.matches("\\d+\\.\\d+\\.\\d+\\.\\d+(:\\d+)?");

            // Resolve non-IP addresses to sort lexicographically if needed
            if (resolveIp) {
                if (!isIp1 && isIp2) return -1;
                if (isIp1 && !isIp2) return 1;
                if (!isIp1 && !isIp2) return key1.compareTo(key2);
            }

            // Split IPs and compare individual parts numerically
            String[] ipPort1 = key1.split(":");
            String[] ipPort2 = key2.split(":");
            String[] parts1 = ipPort1[0].split("\\.");
            String[] parts2 = ipPort2[0].split("\\.");
            for (int i = 0; i < 4; i++) {
                int num1 = Integer.parseInt(parts1[i]);
                int num2 = Integer.parseInt(parts2[i]);
                if (num1 != num2) return Integer.compare(num1, num2);
            }

            // Compare port numbers if present
            if (ipPort1.length > 1 && ipPort2.length > 1) {
                int port1 = Integer.parseInt(ipPort1[1]);
                int port2 = Integer.parseInt(ipPort2[1]);
                return Integer.compare(port1, port2);
            }

            // Handle cases where only one key has a port
            if (ipPort1.length > 1) return 1;
            if (ipPort2.length > 1) return -1;

            return 0;
        });

        // Apply the specified sort order
        applySortOrder(entryList, descending);
    }

    // Generic method to sort entries by a specific index, treating values as strings or percentages
    private void sortByIndex(List<Map.Entry<String, List<Object>>> entryList, int index, boolean descending) {
        entryList.sort((entry1, entry2) -> {
            String str1 = (String) entry1.getValue().get(index);
            String str2 = (String) entry2.getValue().get(index);

            // Handle sorting for percentage values
            if (str1.endsWith("%") && str2.endsWith("%")) {
                double value1 = Double.parseDouble(str1.replace("%", ""));
                double value2 = Double.parseDouble(str2.replace("%", ""));
                return Double.compare(value1, value2);
            }

            // Lexicographical comparison as fallback
            return str1.compareTo(str2);
        });

        // Apply the specified sort order
        applySortOrder(entryList, descending);
    }

    // Method to sort entries by numeric index values
    private void sortByNumericIndex(List<Map.Entry<String, List<Object>>> entryList, int index, boolean descending) {
        entryList.sort((entry1, entry2) -> {
            double value1 = parseSize((String) entry1.getValue().get(index));
            double value2 = parseSize((String) entry2.getValue().get(index));
            return Double.compare(value1, value2);
        });

        // Apply the specified sort order
        applySortOrder(entryList, descending);
    }

    // Reverse the list if descending order is specified
    private void applySortOrder(List<Map.Entry<String, List<Object>>> entryList, boolean descending) {
        if (getSortOrder() == 1) return; // Ascending, no change needed
        if (getSortOrder() == -1 || descending) Collections.reverse(entryList);
    }

    // Print the sorted entries using the provided table builder
    private void sortPrinter(List<Map.Entry<String, List<Object>>> entryList, TableBuilder tableBuilder) {
        for (Map.Entry<String, List<Object>> entry : entryList) {
            String epDns = entry.getKey();
            List<Object> values = entry.getValue();
            String statusAndState = (String) values.get(0);
            String load = (String) values.get(1);
            String rack = (String) values.get(5);

            // Different logic for token per node configurations
            String strOwns, hostID, tokens;
            if (isTokenPerNode) {
                strOwns = (String) values.get(2);
                hostID = (String) values.get(3);
                tokens = (String) values.get(4);
            } else {
                tokens = (String) values.get(2);
                strOwns = (String) values.get(3);
                hostID = (String) values.get(4);
            }

            // Add the row to the table
            if (isTokenPerNode) {
                tableBuilder.add(statusAndState, epDns, load, strOwns, hostID, tokens, rack);
            } else {
                tableBuilder.add(statusAndState, epDns, load, tokens, strOwns, hostID, rack);
            }
        }
    }

    // Determine the sort order based on the user-specified value
    private int getSortOrder() {
        if (sortOrder == null) return 0; // Default: no specific order
        switch (sortOrder) {
            case "a":
            case "ascending":
                return 1; // Ascending order
            case "d":
            case "descending":
                return -1; // Descending order
            default:
                return 3; // Invalid order, treated as no sorting
        }
    }

    // Parse a size string into its numeric value in bytes
    private double parseSize(String sizeStr) {
        if (sizeStr == null || sizeStr.isEmpty()) return 0;

        String[] parts = sizeStr.split(" ");
        if (parts.length != 2) return 0;

        try {
            double value = Double.parseDouble(parts[0]);
            switch (parts[1]) {
                case "KiB":
                    return value * 1024;
                case "MiB":
                    return value * 1024 * 1024;
                case "GiB":
                    return value * 1024 * 1024 * 1024;
                case "TiB":
                    return value * 1024L * 1024 * 1024 * 1024;
                case "PiB":
                    return value * 1024L * 1024 * 1024 * 1024 * 1024;
                default:
                    return value;
            }
        } catch (NumberFormatException e) {
            return 0; // Return zero for invalid numbers
        }
    }
}
