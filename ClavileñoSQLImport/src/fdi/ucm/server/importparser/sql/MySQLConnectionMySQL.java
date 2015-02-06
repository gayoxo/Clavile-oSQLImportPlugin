package fdi.ucm.server.importparser.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Clase que define la conexion a una base de datos mySQL.
 * @author Joaquin Gayoso-Cabada
 *
 */
public class MySQLConnectionMySQL {
	
	
	private static final String ZERO_DATE_TIME_BEHAVIOR_CONVERT_TO_NULL = "?zeroDateTimeBehavior=convertToNull";

	public enum DB {DBaseServer,DBaseLocal};
	
	private Connection conexion;
	private String DBaseServerUnknow;
	
	private String DBSelected;
	private MySQLConnectionMySQL instance;
	
	private static final String DriverDatabase="com.mysql.jdbc.Driver";
	private static final String ErrorMySQLConnection="Error en driver de conexion al mySQL";
	private static final String ErrorCOnexionDB="Error en conexion a base de datos";
	private static final String ErrorUpdate="Error ejecutando Update Querry: ";
	private static final String ErrorSelect="Error ejecutando Querry: ";
	private static final String ErrorTables = "Error recuperando las tablas";
	


	public MySQLConnectionMySQL(String dbNameIP,String database,int Port, String user, String password) {
		try {
			Class.forName(DriverDatabase);
			InicializacionAnonima(dbNameIP,database,Port,user,password); 
			instance=this;
			
		} catch (ClassNotFoundException e) {
			System.err.println(ErrorMySQLConnection);
			e.printStackTrace();
		} catch (SQLException e) {
			System.err.println(ErrorCOnexionDB);
			e.printStackTrace();

		}
	}

	private void InicializacionAnonima(String dbNameIP,String database, int port, String user, String password) throws SQLException {
		DBaseServerUnknow="jdbc:mysql://"+dbNameIP+":"+port+"/"+database+ZERO_DATE_TIME_BEHAVIOR_CONVERT_TO_NULL;
		conexion = DriverManager.getConnection(DBaseServerUnknow, user, password);	
		if (conexion==null) throw new SQLException();
		DBSelected=DBaseServerUnknow;
		
	}
	
	public static MySQLConnectionMySQL getInstance(String dbNameIP,String database,int port, String user, String password) {
		return new MySQLConnectionMySQL(dbNameIP,database,port,user,password);
	}
	
	public void RunQuerryUPDATE(String querry)
	{		
		try {
			Statement st = instance.conexion.createStatement();
			st.executeUpdate(querry);
		} catch (SQLException e) {
			System.err.println(ErrorUpdate + querry);
			e.printStackTrace();
		}
	}
	
	public ResultSet RunQuerrySELECT(String querry)
	{		
		try {
			Statement st = instance.conexion.createStatement();
			ResultSet rs = st.executeQuery(querry);
			return rs;
		} catch (SQLException e) {
			System.err.println(ErrorSelect + querry);
			e.printStackTrace();
			return null;
		}
	}
	
	
	public ResultSet getTables()
	{
		try {
			DatabaseMetaData metaDatos = instance.conexion.getMetaData();
			return metaDatos.getTables(null, null, "%", null);
		} catch (SQLException e) {
			System.err.println(ErrorTables);
			e.printStackTrace();
			return null;
		}
		
	}
	
	public ResultSet getColums(String catalogo,String tabla)
	{
		try {
			DatabaseMetaData metaDatos = instance.conexion.getMetaData();
			return metaDatos.getColumns(catalogo, null, tabla, null);
		} catch (SQLException e) {
			System.err.println(ErrorTables);
			e.printStackTrace();
			return null;
		}
		
	}
	
	public ResultSet getKey(String catalogo,String tabla)
	{
		try {
			DatabaseMetaData metaDatos = instance.conexion.getMetaData();
			return metaDatos.getPrimaryKeys(catalogo, null, tabla);
		} catch (SQLException e) {
			System.err.println(ErrorTables);
			e.printStackTrace();
			return null;
		}
		
	}

	/**
	 * @return the dBSelected
	 */
	public String getDBSelected() {
		return DBSelected;
	}

	/**
	 * @param dBSelected the dBSelected to set
	 */
	public void setDBSelected(String dBSelected) {
		DBSelected = dBSelected;
	}


	 



}
