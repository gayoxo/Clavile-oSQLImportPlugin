/**
 * 
 */
package fdi.ucm.server.importparser.sql;

import java.util.ArrayList;

import fdi.ucm.server.modelComplete.ImportExportDataEnum;
import fdi.ucm.server.modelComplete.ImportExportPair;
import fdi.ucm.server.modelComplete.LoadCollection;
import fdi.ucm.server.modelComplete.collection.CompleteCollectionAndLog;

/**
 * Clase que implementa la creacion de una coleccion basandonos en una BBDD MySQL
 * @author Joaquin Gayoso-Cabada
 *
 */
public class LoadCollectionSQL extends LoadCollection {

	private static ArrayList<ImportExportPair> Parametros;
	
	
	public LoadCollectionSQL() {
		super();
	}
	

	/* (non-Javadoc)
	 * @see fdi.ucm.server.LoadCollection#processCollecccion()
	 */
	@Override
	public CompleteCollectionAndLog processCollecccion(ArrayList<String> DateEntrada) {
		CollectionSQL SQLparser= new CollectionSQL();;
		ArrayList<String> Log=new ArrayList<String>();
		if (DateEntrada!=null)
			
		{
			String Database = RemoveSpecialCharacters(DateEntrada.get(1));
			MySQLConnectionMySQL SQL= MySQLConnectionMySQL.getInstance(DateEntrada.get(0),Database,Integer.parseInt(DateEntrada.get(2)),DateEntrada.get(3),DateEntrada.get(4));
			
			
			Boolean inferRelations=false;
			
			try {
				inferRelations=Boolean.parseBoolean(DateEntrada.get(5));
			} catch (Exception e) {
				
			}
		
			Boolean RelationsPublicPrivate=false;
			
			try {
				RelationsPublicPrivate=Boolean.parseBoolean(DateEntrada.get(6));
			} catch (Exception e) {
				
			}
			
			
			if (inferRelations)
				{
				SQLparser= new CollectionSQLInfering();
				((CollectionSQLInfering)SQLparser).publicPrivateAtribute(RelationsPublicPrivate);
				}
				
			
			SQLparser.setMySQLInstance(SQL);
			SQLparser.ProcessAttributes();
		}
		else 
		{
		Log.add("Warning: Datos de entrada vacios");	
		}
		
		return new CompleteCollectionAndLog(SQLparser.getColeccion(),Log);
	}

	/* (non-Javadoc)
	 * @see fdi.ucm.server.LoadCollection#getConfiguracion()
	 */
	@Override
	public ArrayList<ImportExportPair> getConfiguracion() {
		if (Parametros==null)
		{
			ArrayList<ImportExportPair> ListaCampos=new ArrayList<ImportExportPair>();
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.Text, "MySQL Server Direction"));
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.Text, "MySQL Database"));
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.Number, "MySQL Port"));
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.Text, "MySQL User"));
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.EncriptedText, "MySQL Password"));
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.Boolean, "Infiering Relations (A_B)"));
			ListaCampos.add(new ImportExportPair(ImportExportDataEnum.Boolean, "Public/Private Relations (Private_A_B=>Private_A_Private_B)"));
			Parametros=ListaCampos;
			return ListaCampos;
		}
		else return Parametros;
	}

	/* (non-Javadoc)
	 * @see fdi.ucm.server.LoadCollection#getName()
	 */
	@Override
	public String getName() {
		return "MySQL";
	}
	
	/**
	 * QUitar caracteres especiales.
	 * @param str texto de entrada.
	 * @return texto sin caracteres especiales.
	 */
	public String RemoveSpecialCharacters(String str) {
		   StringBuilder sb = new StringBuilder();
		   for (int i = 0; i < str.length(); i++) {
			   char c = str.charAt(i);
			   if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_') {
			         sb.append(c);
			      }
		}
		   return sb.toString();
		}


	@Override
	public boolean getCloneLocalFiles() {
		return false;
	}

}
