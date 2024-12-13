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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import com.google.common.collect.ArrayListMultimap;

import static java.util.stream.Collectors.toMap;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
@Command(name = "status", description = "Print cluster information (state, load, IDs, ...)")
public class Status extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace>]", description = "The keyspace name")
    private String keyspace = null;

    @Option(title = "resolve_ip", name = {"-r", "--resolve-ip"}, description = "Show node domain names instead of IPs")
    private boolean resolveIp = false;

    @Option(title = "sort",
            name = {"-s", "--sort"},
            description = "Sort by one of 'ip', 'load', 'owns', 'id', 'rack' or 'state', Defaults to 'none', Default Ordering is 'asc' for 'ip', 'id', 'rack' and 'desc' for 'load', 'owns', 'state' ",
            allowedValues = {"ip", "load", "owns", "id", "rack", "state", "none"})
    private SortBy sortBy = SortBy.none;

    @Option(title = "sort_order",
            name = {"-o", "--order"},
            description = "Sort order: 'asc' for ascending, 'desc' for descending",
            allowedValues = {"asc", "desc"})
    private SortOrder sortOrder = null;

    private boolean isTokenPerNode = true;
    private Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
    private Map<String, String> loadMap, hostIDMap;
    private EndpointSnitchInfoMBean epSnitchInfo;

    public enum SortOrder
    {
        asc,
        desc
    }

    public enum SortBy
    {
        none,
        ip,
        load,
        owns,
        id,
        rack,
        state
    }

    private void validateArguments(PrintStream out)
    {
        if (sortOrder != null && sortBy == SortBy.none)
        {
            out.printf("%nError: %s%n", "Sort order (-o / --order) can only be used while sorting using -s or --sort.");
            System.exit(1);
        }
    }

    @Override
    public void execute(NodeProbe probe)
    {
        validateArguments(probe.output().out);

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

        SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

        // More tokens than nodes (aka vnodes)?
        if (dcs.size() < tokensToEndpoints.size())
            isTokenPerNode = false;

        // Datacenters
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            TableBuilder tableBuilder = sharedTable.next();
            addNodesHeader(hasEffectiveOwns, tableBuilder);

            ArrayListMultimap<InetAddressAndPort, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
            for (HostStatWithPort stat : dc.getValue())
                hostToTokens.put(stat.endpointWithPort, stat);

            Map<String, List<Object>> data = new HashMap<>();
            for (InetAddressAndPort endpoint : hostToTokens.keySet())
            {
                Float owns = ownerships.get(endpoint.getHostAddressAndPort());
                List<HostStatWithPort> tokens = hostToTokens.get(endpoint);

                HostStatWithPort hostStatWithPort = tokens.get(0);
                String epDns = hostStatWithPort.ipOrDns(printPort);
                List<Object> nodeData = addNode(epDns, endpoint, owns, hostStatWithPort, tokens.size(), hasEffectiveOwns);
                data.put(epDns, nodeData);
            }

            data = sort(data);

            for (Map.Entry<String, List<Object>> entry : data.entrySet())
            {
                List<Object> values = entry.getValue();
                List<String> row = new ArrayList<>();
                for (int i = 1; i < values.size(); i++)
                    row.add((String) values.get(i));

                tableBuilder.add(row);
            }
        }

        Iterator<TableBuilder> results = sharedTable.complete().iterator();
        boolean first = true;
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            if (!first)
                out.println();
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

    private List<Object> addNode(String epDns, InetAddressAndPort addressAndPort, Float owns, HostStatWithPort hostStat, int size, boolean hasEffectiveOwns)
    {
        String endpoint = addressAndPort.getHostAddressAndPort();
        String status, state, load, strOwns, hostID, rack;
        if (liveNodes.contains(endpoint))
            status = "U";
        else if (unreachableNodes.contains(endpoint))
            status = "D";
        else
            status = "?";
        if (joiningNodes.contains(endpoint))
            state = "J";
        else if (leavingNodes.contains(endpoint))
            state = "L";
        else if (movingNodes.contains(endpoint))
            state = "M";
        else
            state = "N";

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

        if (isTokenPerNode)
            return List.of(addressAndPort, statusAndState, epDns, load, strOwns, hostID, hostStat.token, rack);
        else
            return List.of(addressAndPort, statusAndState, epDns, load, String.valueOf(size), strOwns, hostID, rack);
    }

    // To check for descending order
    private Boolean desc()
    {
        return sortOrder == null ? null : sortOrder == SortOrder.desc;
    }

    // Sort function to sort the data
    private Map<String, List<Object>> sort(Map<String, List<Object>> map)
    {
        switch (sortBy)
        {
            case none:
                return map;
            case ip:
                return sortByIp(map);
            case load:
                return sortByLoad(map);
            case id:
                return sortById(map);
            case rack:
                return sortByRack(map);
            case owns:
                return sortByOwns(map);
            case state:
                return sortByState(map);
            default:
                throw new IllegalArgumentException("Sorting by " + sortBy + " is not supported.");
        }
    }

    // Helper Function to Sort by Ip
    private LinkedHashMap<String, List<Object>> sortByIp(Map<String, List<Object>> data)
    {
        int index = 0;
        boolean desc = desc() != null ? desc() : false; // default is ascending
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    InetAddressAndPort addr1 = (InetAddressAndPort) e1.getValue().get(index);
                    InetAddressAndPort addr2 = (InetAddressAndPort) e2.getValue().get(index);
                    return desc ? addr2.compareTo(addr1) : addr1.compareTo(addr2);
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    // Helper Function to Sort by Load
    private LinkedHashMap<String, List<Object>> sortByLoad(Map<String, List<Object>> data)
    {
        int index = 3;
        boolean desc = desc() != null ? desc() : true; // defalut is descending
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    String str1 = (String) e1.getValue().get(index);
                    String str2 = (String) e2.getValue().get(index);
                    // Check if str1 or str2 contains a '?' and set a value for it.
                    boolean containsQuestionMark1 = str1.contains("?");
                    boolean containsQuestionMark2 = str2.contains("?");

                    if (containsQuestionMark1 && containsQuestionMark2) // If both contain '?', return 0 (they are considered equal).
                        return 0;

                    if (containsQuestionMark1) // If str1 contains '?', ensure it's last (or first depending on descending).
                        return desc ? 1 : -1;

                    if (containsQuestionMark2) // If str2 contains '?', ensure it's last (or first depending on descending).
                        return desc ? -1 : 1;

                    // If neither contain '?', parse the file sizes and compare.
                    long value1 = FileUtils.parseFileSize((String) e1.getValue().get(index));
                    long value2 = FileUtils.parseFileSize((String) e2.getValue().get(index));
                    return desc ? Long.compare(value2, value1) : Long.compare(value1, value2);
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    // Helper Function to Sort by Host ID
    private LinkedHashMap<String, List<Object>> sortById(Map<String, List<Object>> data)
    {
        int index = isTokenPerNode ? 5 : 6;
        boolean desc = desc() != null ? desc() : false; // default is ascending
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    String str1 = (String) e1.getValue().get(index);
                    String str2 = (String) e2.getValue().get(index);
                    return desc ? str2.compareTo(str1) : str1.compareTo(str2);
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    // Helper Function to Sort by Rack
    private LinkedHashMap<String, List<Object>> sortByRack(Map<String, List<Object>> data)
    {
        int index = 7;
        boolean desc = desc() != null ? desc() : false; // default is ascending
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    String str1 = (String) e1.getValue().get(index);
                    String str2 = (String) e2.getValue().get(index);
                    return desc ? str2.compareTo(str1) : str1.compareTo(str2);
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    // Helper Function to Sort by Owns
    private LinkedHashMap<String, List<Object>> sortByOwns(Map<String, List<Object>> data)
    {
        int index = isTokenPerNode ? 4 : 5;
        boolean desc = desc() != null ? desc() : true; // default is descending
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    double value1 = Double.parseDouble(((String) e1.getValue().get(index)).replace("%", ""));
                    double value2 = Double.parseDouble(((String) e2.getValue().get(index)).replace("%", ""));
                    return desc ? Double.compare(value2, value1) : Double.compare(value1, value2);
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    // Helper Function to Sort by State
    private LinkedHashMap<String, List<Object>> sortByState(Map<String, List<Object>> data)
    {
        int index = 1;
        boolean desc = desc() != null ? desc() : true; // default is descending
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    String str1 = (String) e1.getValue().get(index);
                    String str2 = (String) e2.getValue().get(index);
                    return desc ? str2.compareTo(str1) : str1.compareTo(str2);
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

}