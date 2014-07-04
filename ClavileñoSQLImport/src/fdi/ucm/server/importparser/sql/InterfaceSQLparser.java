package fdi.ucm.server.importparser.sql;

/**
 * Interface parseModel, funciones necesarias para parseal un objeto, parsear su modelo y sus instancias
 * @author Joaquin Gayoso-Cabada
 *
 */
public interface InterfaceSQLparser {

	/**
	 * Funcion inicial del proceso del modelo.
	 */
	public void ProcessAttributes();
	
	/**
	 * Funcion inicial del proceso las instancias.
	 */
	public void ProcessInstances();
	
	/**
	 * Funcion inicial del proceso las instancias.
	 */
	public void setMySQLInstance(MySQLConnectionMySQL MySQL);
}
