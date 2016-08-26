package com.flytxt.commons.voltdb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ConfigurationProperties(prefix="voltdb")
@Data
public class VoltDbInteractorImpl implements VoltDbInteractor {

	private static Client client;

	private static ClientConfig config;

	private String username;

	private String password;
	
	private List<VoltdbNode> nodes;
	
	

	@Override
	public ClientResponse executeProcedure(String procedure, Object... args) {
		ClientResponse callProcedure;
		try {
			callProcedure = client.callProcedure(procedure, args);
			return callProcedure;
		}
		catch (IOException | ProcCallException e) {
			log.error("Error executing procedure {}", procedure);
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean executeProcedure(ProcedureCallback callback, String procedure, Object... args) {
		boolean callProcedure;
		try {
			callProcedure = client.callProcedure(callback, procedure, args);
			return callProcedure;
		}
		catch (IOException e) {
			log.error("Error executing procedure {}", procedure);
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	public VoltDbInteractorImpl() {

	}

	@PostConstruct
	private void init() throws UnknownHostException, IOException {
		if (username != null && !username.isEmpty()) {
			config = new ClientConfig(username, password);
		}
		else {
			config = new ClientConfig();
		}
		config.setProcedureCallTimeout(0);
		config.setReconnectOnConnectionLoss(true);
		client = ClientFactory.createClient(config);
		for (VoltdbNode node : nodes) {
			client.createConnection(node.getHost(), node.getPort());
		}

	}

	public VoltDbInteractorImpl(Properties voltdbProperties) throws NumberFormatException, UnknownHostException, IOException {
		this(voltdbProperties.getProperty("ip"), Integer.parseInt(voltdbProperties.getProperty("port")), voltdbProperties.getProperty("user"),
				voltdbProperties.getProperty("password"));
	}

	public VoltDbInteractorImpl(String ip, int port, String user, String password) throws UnknownHostException, IOException {
		if (null != user && !user.isEmpty())
			config = new ClientConfig(user, password);
		else
			config = new ClientConfig();
		config.setProcedureCallTimeout(0);
		config.setReconnectOnConnectionLoss(true);
		client = ClientFactory.createClient(config);
		if (ip.contains(",")) {
			List<String> ipList = Arrays.asList(ip.split(","));
			for (String ip_addr : ipList) {
				client.createConnection(ip_addr, port);
			}
		}
		else {
			client.createConnection(ip, port);
		}

	}

}
