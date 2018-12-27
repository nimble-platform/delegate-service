package eu.nimble.service.delegate;


public class EurekaEntry {
	private String name;
	private String ip;
	private Status status;
	
	public EurekaEntry(String name, String ip, Status status) {
		this.name = name;
		this.ip = ip;
		this.status = status;
	}

	public String getName() {
		return name;
	}

	public String getIp() {
		return ip;
	}

	public Status getStatus() {
		return status;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EurekaEntry other = (EurekaEntry) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (status != other.status)
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "EurekaEntry [name=" + name + ", ip=" + ip + ", status=" + status + "]";
	}

	public static enum Status {
		UP,
		DOWN,
		OUT_OF_SERVICE
	}
}
