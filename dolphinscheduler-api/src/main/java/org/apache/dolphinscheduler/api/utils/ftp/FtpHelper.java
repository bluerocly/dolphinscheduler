package org.apache.dolphinscheduler.api.utils.ftp;

import java.io.InputStream;

public abstract class FtpHelper {
	/**
	 * 
	* @Title: LoginFtpServer 
	* @Description: 与ftp服务器建立连接
	* @param @param host
	* @param @param username
	* @param @param password
	* @param @param port
	* @param @param timeout
	* @param @param connectMode     
	* @return void 
	* @throws
	 */
	public abstract void loginFtpServer(String host, String username, String password, int port, int timeout,String connectMode) throws Exception;
	/**
	 * 
	* @Title: LogoutFtpServer 
	* todo 方法名首字母
	* @Description: 断开与ftp服务器的连接 
	* @param      
	* @return void 
	* @throws
	 */
	public abstract void logoutFtpServer();
	/**
	 * 
	* @Title: isDirExist 
	* @Description: 判断指定路径是否是目录
	* @param @param directoryPath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public abstract boolean isDirExist(String directoryPath);
	/**
	 * 
	* @Title: isFileExist 
	* @Description: 判断指定路径是否是文件
	* @param @param filePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public abstract boolean isFileExist(String filePath);
	/**
	 * 
	* @Title: isSymbolicLink 
	* @Description: 判断指定路径是否是软链接
	* @param @param filePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public abstract boolean isSymbolicLink(String filePath);
	
	/**
	 * 
	* @Title: getInputStream 
	* @Description: 获取指定路径的输入流
	* @param @param filePath
	* @param @return     
	* @return InputStream 
	* @throws
	 */
	public abstract InputStream getInputStream(String filePath);
	

}
