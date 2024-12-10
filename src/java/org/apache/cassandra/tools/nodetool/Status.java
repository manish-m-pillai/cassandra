
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
            description = "Sort by one of 'ip', 'load', 'owns', 'id', 'rack' or 'status', Defaults to 'none', Default Ordering is 'asc' for 'ip', 'id', 'rack' and 'desc' for 'load', 'owns', 'status' ",
            allowedValues = { "ip", "load", "owns", "id", "rack", "status", "none" })
    private SortBy sortBy = SortBy.none;

    @Option(title = "sort_order",
            name = {"-o","--order"},
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
        status
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

    private List<Object> addNode(String epDns, InetAddressAndPort addressAndPort, Float owns, HostStatWithPort hostStat, int size, boolean hasEffectiveOwns)
    {
        String endpoint = addressAndPort.getHostAddressAndPort();
        String status, state, load, strOwns, hostID, rack;
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

        if (isTokenPerNode)
            return List.of(addressAndPort, statusAndState, epDns, load, strOwns, hostID, hostStat.token, rack);
        else
            return List.of(addressAndPort, statusAndState, epDns, load, String.valueOf(size), strOwns, hostID, rack);
    }

    private Boolean desc()
    {
        if(sortOrder==sortOrder.desc)
        {
            return true;
        }
        else if(sortOrder==sortOrder.asc)
        {
            return false;
        }
        else
        {
            return null;
        }
    }

    private Map<String, List<Object>> sort(Map<String, List<Object>> map)
    {
        switch (sortBy)
        {
            case none:
                return map;
            case ip:
                return sortInternal(map, SortBy.ip, 0, desc() != null ? desc() : false); // default order is ascending
            case load:
                return sortInternal(map, SortBy.load, 3, desc() != null ? desc() : true); // default order is descending
            case id:
                return sortInternal(map, SortBy.id, isTokenPerNode ? 5 : 6, desc() != null ? desc() : false); // default order is ascending
            case rack:
                return sortInternal(map, SortBy.rack, 7, desc() != null ? desc() : false); // default order is ascending
            case owns:
                return sortInternal(map, SortBy.owns, isTokenPerNode ? 4 : 5, desc() != null ? desc() : true); // default order is descending
            case status:
                return  sortInternal(map, SortBy.status, 1, desc() != null ? desc() : true); // default order is descending
            default:
                throw new IllegalArgumentException("Sorting by " + sortBy + " is not supported.");
        }
    }

    private LinkedHashMap<String, List<Object>> sortInternal(Map<String, List<Object>> data, SortBy sortBy, int index, boolean descending)
    {
        return data.entrySet()
                .stream()
                .sorted((e1, e2) -> {
                    if (sortBy == SortBy.ip)
                    {
                        InetAddressAndPort addr1 = (InetAddressAndPort) e1.getValue().get(index);
                        InetAddressAndPort addr2 = (InetAddressAndPort) e2.getValue().get(index);
                        // if ip's are resolved, strings will be DNS names
                        if (resolveIp)
                        {
                            String str1 = addr1.getHostAddressAndPort();
                            String str2 = addr2.getHostAddressAndPort();

                            if (descending)
                                return str2.compareTo(str1);
                            else
                                return str1.compareTo(str2);
                        }
                        else
                        {
                            if (descending)
                                return addr2.compareTo(addr1);
                            else
                                return addr1.compareTo(addr2);
                        }
                    }

                    String str1 = (String) e1.getValue().get(index);
                    String str2 = (String) e2.getValue().get(index);

                    if (sortBy == SortBy.owns)
                    {
                        double value1 = Double.parseDouble(str1.replace("%", ""));
                        double value2 = Double.parseDouble(str2.replace("%", ""));

                        if (descending)
                            return Double.compare(value2, value1);
                        else
                            return Double.compare(value1, value2);
                    }
                    else if (sortBy == SortBy.load)
                    {
                        long value1 = FileUtils.parseFileSize(str1);
                        long value2 = FileUtils.parseFileSize(str2);

                        if (descending)
                            return Long.compare(value2, value1);
                        else
                            return Long.compare(value1, value2);
                    }
                    // Lexicographical comparison as fallback
                    else if (descending)
                    {
                        return str2.compareTo(str1);
                    }
                    else
                    {
                        return str1.compareTo(str2);
                    }
                }).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
}
