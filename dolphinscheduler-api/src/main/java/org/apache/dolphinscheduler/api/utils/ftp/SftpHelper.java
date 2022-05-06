package org.apache.dolphinscheduler.api.utils.ftp;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class SftpHelper extends FtpHelper {
	private static final Logger LOG = LoggerFactory.getLogger(SftpHelper.class);

	Session session = null;
	ChannelSftp channelSftp = null;
	@Override
	public void loginFtpServer(String host, String username, String password, int port, int timeout,
			String connectMode) throws Exception {
		JSch jsch = new JSch(); // 创建JSch对象
		try {
			session = jsch.getSession(username, host, port);
			// 根据用户名，主机ip，端口获取一个Session对象
			// 如果服务器连接不上，则抛出异常
			if (session == null) {
				throw new Exception("session is null,无法通过sftp与服务器建立链接，请检查主机名和用户名是否正确.");
			}

			session.setPassword(password); // 设置密码
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config); // 为Session对象设置properties
			session.setTimeout(timeout); // 设置timeout时间
			session.connect(); // 通过Session建立链接

			channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
			channelSftp.connect(); // 建立SFTP通道的连接
			
			//设置命令传输编码
			//String fileEncoding = System.getProperty("file.encoding");
			//channelSftp.setFilenameEncoding(fileEncoding);		
		} catch (JSchException e) {
			if(null != e.getCause()){
				String cause = e.getCause().toString();
				String unknownHostException = "java.net.UnknownHostException: " + host;
				String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:" + port;
				String wrongPort = "java.net.ConnectException: Connection refused";
				if (unknownHostException.equals(cause)) {
					String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
					LOG.error(message);
					throw new Exception(message);
				} else if (illegalArgumentException.equals(cause) || wrongPort.equals(cause) ) {
					String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
					LOG.error(message);
					throw new Exception(message);
				}
			} else {
				if("Auth fail".equals(e.getMessage())){
					String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
							"message:host =" + host + ",username = " + username + ",port =" + port);
					LOG.error(message);
					throw new Exception(message);
				}else{
					String message = String.format("与ftp服务器建立连接失败 : [%s]",
							"message:host =" + host + ",username = " + username + ",port =" + port);
					LOG.error(message);
					throw new Exception(message);
				}				
			}
		}

	}

	@Override
	public void logoutFtpServer() {
		if (channelSftp != null) {
			channelSftp.disconnect();
		}
		if (session != null) {
			session.disconnect();
		}
	}

	@Override
	public boolean isDirExist(String directoryPath) {
		try {
			SftpATTRS sftpATTRS = channelSftp.lstat(directoryPath);
			return sftpATTRS.isDir();
		} catch (SftpException e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directoryPath);
				LOG.error(message);
				return false;
//				throw new Exception(message);
			}
			String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
			LOG.error(message);
			return false;
//			throw new Exception(message);
		}
	}

	@Override
	public boolean isFileExist(String filePath) {
		boolean isExitFlag = false;	
		try {
			SftpATTRS sftpATTRS = channelSftp.lstat(filePath);			
			if(sftpATTRS.getSize() >= 0){
				isExitFlag = true;
			}
		} catch (SftpException e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
				LOG.error(message);
//				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			} else {
				String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
				LOG.error(message);
//				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
		}
		return isExitFlag;
	}

	@Override
	public boolean isSymbolicLink(String filePath) {
		try {
			SftpATTRS sftpATTRS = channelSftp.lstat(filePath);
			return sftpATTRS.isLink();
		} catch (SftpException e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
				LOG.error(message);
//				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
				return false;
			} else {
				String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
				LOG.error(message);
				return false;
//				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
		}
	}

	@Override
	public InputStream getInputStream(String filePath) {
		try {
			return channelSftp.get(filePath);
		} catch (SftpException e) {
			String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
			LOG.error(message);
			return null;
//			throw DataXException.asDataXException(FtpReaderErrorCode.OPEN_FILE_ERROR, message);
		}
	}

}
