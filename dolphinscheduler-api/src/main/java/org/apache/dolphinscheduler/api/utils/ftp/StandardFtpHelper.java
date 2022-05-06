package org.apache.dolphinscheduler.api.utils.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StandardFtpHelper extends FtpHelper {
	private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelper.class);
	FTPClient ftpClient = null;

	@Override
	public void loginFtpServer(String host, String username, String password, int port, int timeout,
			String connectMode) throws Exception {
		ftpClient = new FTPClient();
		try {
			// 连接
			ftpClient.connect(host, port);
			// 登录
			ftpClient.login(username, password);
			// 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
			// ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			ftpClient.setConnectTimeout(timeout);
			ftpClient.setDataTimeout(timeout);
			if ("PASV".equals(connectMode)) {
				ftpClient.enterRemotePassiveMode();
				ftpClient.enterLocalPassiveMode();
			} else if ("PORT".equals(connectMode)) {
				ftpClient.enterLocalActiveMode();
				// ftpClient.enterRemoteActiveMode(host, port);
			}
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
						"message:host =" + host + ",username = " + username + ",port =" + port);
				LOG.error(message);
				throw new Exception(message);
			}
			//设置命令传输编码
			String fileEncoding = System.getProperty("file.encoding");
			ftpClient.setControlEncoding(fileEncoding);
		} catch (UnknownHostException e) {
			String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
			LOG.error(message);
			throw new Exception(message);
		} catch (IllegalArgumentException e) {
			String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
			LOG.error(message);
			throw new Exception(message);
		} catch (Exception e) {
			String message = String.format("与ftp服务器建立连接失败 : [%s]",
					"message:host =" + host + ",username = " + username + ",port =" + port);
			LOG.error(message);
			throw new Exception(message);
		}

	}

	@Override
	public void logoutFtpServer() {
		if (ftpClient.isConnected()) {
			try {
				//todo ftpClient.completePendingCommand();//打开流操作之后必须，原因还需要深究
				ftpClient.logout();
			} catch (IOException e) {
				String message = "与ftp服务器断开连接失败";
				LOG.error(message);
			}finally {
				if(ftpClient.isConnected()){
					try {
						ftpClient.disconnect();
					} catch (IOException e) {
						String message = "与ftp服务器断开连接失败";
						LOG.error(message);
					}
				}

			}
		}
	}

	@Override
	public boolean isDirExist(String directoryPath) {
		try {
			return ftpClient.changeWorkingDirectory(new String(directoryPath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
		} catch (IOException e) {
			String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
			LOG.error(message);
			return false;
//			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
	}

	@Override
	public boolean isFileExist(String filePath) {
		boolean isExitFlag = false;
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
			if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
			LOG.error(message);
			return false;
//			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
		return isExitFlag;
	}

	@Override
	public boolean isSymbolicLink(String filePath) {
		boolean isExitFlag = false;
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
			if (ftpFiles.length == 1 && ftpFiles[0].isSymbolicLink()) {
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
			LOG.error(message);
//			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
		return isExitFlag;
	}


	@Override
	public InputStream getInputStream(String filePath) {
		try {
			return ftpClient.retrieveFileStream(new String(filePath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
		} catch (IOException e) {
			String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
			LOG.error(message);
			return null;
//			throw DataXException.asDataXException(FtpReaderErrorCode.OPEN_FILE_ERROR, message);
		}
	}

}
