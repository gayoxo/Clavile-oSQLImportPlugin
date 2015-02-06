/**
 * 
 */
package fdi.ucm.server.importparser.sql;

/**
 * @author Joaquin Gayoso-Cabada
 *
 */
public class KeyElement {

	private String NombreTabla;
	private String NombreColumna;
	private String PKName;
	
	public KeyElement(String nombreTabla, String nombrecolumna, String pKName) {
		NombreTabla=nombreTabla;
		NombreColumna=nombrecolumna;
		PKName=pKName;
	}
	
	
	public String getNombreTabla() {
		return NombreTabla;
	}
	
	public void setNombreTabla(String nombreTabla) {
		NombreTabla = nombreTabla;
	}


	public String getNombreColumna() {
		return NombreColumna;
	}


	public void setNombreColumna(String nombreColumna) {
		NombreColumna = nombreColumna;
	}


	public String getPKName() {
		return PKName;
	}


	public void setPKName(String pKName) {
		PKName = pKName;
	}

	
	
}
