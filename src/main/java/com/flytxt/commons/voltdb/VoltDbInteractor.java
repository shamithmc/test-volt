package com.flytxt.commons.voltdb;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public interface VoltDbInteractor {
	
	public ClientResponse executeProcedure(String procedure, Object... args);

	boolean executeProcedure(ProcedureCallback callback, String procedure, Object... args);
}
