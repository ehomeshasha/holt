package ca.dealsaccess.holt.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZKClient {

	private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
	
	private ZooKeeper zk;
	
	private int CONNECTION_TIMEOUT = 30000;
	
	private final List<String> zkHostsList;
	
	@SuppressWarnings("serial")
	public static class ZKClientException extends Exception {
		public ZKClientException(String msg) {
			super(msg);
		}

		public ZKClientException(String msg, Exception e) {
			super(msg, e);
		}
	}
	
	public ZKClient(List<String> zkHostsList) throws IOException {
		this.zkHostsList = zkHostsList;
		Collections.shuffle(this.zkHostsList);
		try {
			zk = new ZooKeeper(this.zkHostsList.get(0), CONNECTION_TIMEOUT,  
					new Watcher() { 
			    public void process(WatchedEvent event) { 
			    	LOG.info("{} event has been triggered ", event.getType()); 
			    } 
			});
		} catch (IOException e) {
			throw new IOException("Zookeeper Server can not connected");
		} 
	}
	
	public boolean createZnode(String path) {
		return createZnode(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	private boolean createZnode(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
		RecursiveCreateZnode rcz = new RecursiveCreateZnode(path);
		try {
			return rcz.create();
		} catch (KeeperException e) {
			LOG.warn("create node "+path+" failed", e);
			return false;
		} catch (InterruptedException e) {
			LOG.warn("Unexpected InterruptedException occurs when creating node {}", path);
			return false;
		}
	}
	
	private class RecursiveCreateZnode {
		private final Logger LOG = LoggerFactory.getLogger(RecursiveCreateZnode.class);
		
		private String path;
		private byte[] data;
		private List<ACL> acl;
		private CreateMode createMode;
		
		private RecursiveCreateZnode(String path) {
			this(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		public RecursiveCreateZnode(String path, byte[] data, List<ACL> acl,
				CreateMode createMode) {
			this.path = path;
			this.data = data;
			this.acl = acl;
			this.createMode = createMode;
		}

		public boolean create() throws KeeperException, InterruptedException {
			PathUtils.validatePath(path, false);
			if(zNodeExist(path)) {
				LOG.warn("node {} already exists, so nothing to do");
				return true;
			}
			return recursiveCreateZnode(path, data, acl, createMode);
		}
		
		private boolean recursiveCreateZnode(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
			File f = new File(path);
			String parentPath = f.getParent();
			if(parentPath.equals("/")) {
				return bulkCreateNodes("", data, acl, createMode);
			}
			
			if(zNodeExist(parentPath)) {
				return bulkCreateNodes(parentPath, data, acl, createMode);
			}
			recursiveCreateZnode(parentPath, data, acl, createMode);
			return true;
		}
		
		
		private boolean CreateZnode(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
			try {
				String newPath = zk.create(path, data, acl, createMode);
				LOG.info("Created " + newPath);
				return true;
			} catch (KeeperException.InvalidACLException ex) {
				LOG.error(ex.getMessage());
				return false;
			}
		}


		private boolean zNodeExist(String path) throws KeeperException, InterruptedException {
			return zk.exists(path, null) != null ? true : false;
		}
		
		private boolean bulkCreateNodes(String parentPath, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
			
			String[] pathArr = path.replaceFirst(parentPath, "").substring(1).split("/");
			StringBuilder sb = new StringBuilder(parentPath);
			boolean flag = true;
			for(int i=0;i<pathArr.length;i++) {
				String createPath = sb.append("/").append(pathArr[i]).toString();
				int tryCount = 0;
				while(true) {
					if(CreateZnode(createPath, data, acl, createMode)) {
						break;
					}
					if(tryCount > 10) {
						flag = false;
					}
					tryCount++;
				}
			}
			return flag;
		}
	}
	 
	
	/*

	// 创建一个与服务器的连接
	 ZooKeeper 
	 // 创建一个目录节点
	 zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE,
	   CreateMode.PERSISTENT); 
	 // 创建一个子目录节点
	 zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
	   Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
	 System.out.println(new String(zk.getData("/testRootPath",false,null))); 
	 // 取出子目录节点列表
	 System.out.println(zk.getChildren("/testRootPath",true)); 
	 // 修改子目录节点数据
	 zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1); 
	 System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]"); 
	 // 创建另外一个子目录节点
	 zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(), 
	   Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
	 System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo",true,null))); 
	 // 删除子目录节点
	 zk.delete("/testRootPath/testChildPathTwo",-1); 
	 zk.delete("/testRootPath/testChildPathOne",-1); 
	 // 删除父目录节点
	 zk.delete("/testRootPath",-1); 
	 // 关闭连接
	 zk.close();
	 */ 
}
