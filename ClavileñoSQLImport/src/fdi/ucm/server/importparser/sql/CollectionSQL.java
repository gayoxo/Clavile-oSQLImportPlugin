/**
 * 
 */
package fdi.ucm.server.importparser.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import fdi.ucm.server.modelComplete.collection.CompleteCollection;
import fdi.ucm.server.modelComplete.collection.document.CompleteDocuments;
import fdi.ucm.server.modelComplete.collection.document.CompleteElement;
import fdi.ucm.server.modelComplete.collection.document.CompleteTextElement;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteElementType;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteGrammar;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteOperationalValueType;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteTextElementType;

/**
 * Clase que implementa la creacion de la base de datos per se
 * @author Joaquin Gayoso-Cabada
 *
 */
public class CollectionSQL implements InterfaceSQLparser {

	protected static final String COLECCION_A_APARTIR_DE_UN_SQL = "Coleccion a apartir de un SQL : ";
	protected static final String SQL_COLLECTION = "SQL Collection";
	protected CompleteCollection coleccionstatica;
	protected static enum Numbers {TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, DECIMAL,DEC,FLOAT,DOUBLE};
	protected static enum Fecha {DATETIME,
		//'0000-00-00 00:00:00'
							   DATE,
		//'0000-00-00'
							   TIMESTAMP,
		//00000000000000	   
							   TIME,
		//'00:00:00'
							   YEAR
	};
	
	protected static enum Texto {CHAR, VARCHAR, BINARY, VARBINARY, BLOB, TEXT, ENUM,LONGTEXT};
	
	protected static enum Booleanos {TINYINT,BOOL,BOOLEAN,BIT};
	
	protected static enum Controlado {SET};
	

	protected MySQLConnectionMySQL MySQL;
	protected HashMap<String, HashMap<String, Integer>> ClaveClaves;
	protected int Contadordetablas;
	
	protected HashMap<CompleteGrammar,HashMap<String, CompleteDocuments>> TablaEquivalDocu;
	
	public CollectionSQL() {
		coleccionstatica=new CompleteCollection(SQL_COLLECTION, COLECCION_A_APARTIR_DE_UN_SQL+ new Timestamp(new Date().getTime()));
		TablaEquivalDocu=new HashMap<CompleteGrammar, HashMap<String,CompleteDocuments>>();
	}
	
	/* (non-Javadoc)
	 * @see fdi.ucm.server.importparser.sql.SQLparserModel#ProcessAttributes()
	 */
	@Override
	public void ProcessAttributes() {
		try {
			ClaveClaves=new HashMap<String, HashMap<String, Integer>>();
			Contadordetablas=0;
			ResultSet rs=MySQL.getTables();
			if (rs!=null) 
			{
				while (rs.next()) {
					   String catalogo = rs.getString(1);
					   String tabla = rs.getString(3);
					   CompleteGrammar M=new CompleteGrammar(tabla, tabla,coleccionstatica);
					   
					   if (ClaveClaves.get(tabla)==null)
						   ClaveClaves.put(tabla, new HashMap<String, Integer>());
						
					   String VistaOV=new String(NameConstantsSQL.SQL);
						CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.SQLType,NameConstantsSQL.TABLA,VistaOV);
						M.getViews().add(Valor);

						
					   coleccionstatica.getMetamodelGrammar().add(M);
					   ArrayList<CompleteElementType> MetaColumnas=procesaColumnas(catalogo,tabla,M);
					   procesaColumnasInstancia(catalogo,tabla,MetaColumnas,M);
					}
			rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Procea las instancias de las columnas
	 * @param catalogo
	 * @param tabla
	 * @param metaColumnas 
	 * @param Documento 
	 */
	protected void procesaColumnasInstancia(String catalogo, String tabla, ArrayList<CompleteElementType> metaColumnas, CompleteGrammar Documento) {
		try {
			ResultSet rs=MySQL.RunQuerrySELECT("SELECT * FROM "+ tabla +";");
			if (rs!=null) 
			{
				while (rs.next()) {
					
					String ID=rs.getString(1);
					
					CompleteDocuments DocumentosC=new CompleteDocuments(coleccionstatica,"Columna","");
					for (CompleteElementType completeElementType : metaColumnas) {
						Object O=null;
						try {
							O=rs.getObject(completeElementType.getName());
						} catch (Exception e) {
							e.printStackTrace();
							
						}
						
						
						if (O!=null)
							{
							String Dato=O.toString();
							if (Dato!=null&&!Dato.isEmpty())
								{
								CompleteElement MV=generaMetavalue(completeElementType,Dato);
								if (MV!=null)
									DocumentosC.getDescription().add(MV);
								}
							}
					}
					if (DocumentosC.getDescription().size()>0)
						{
						coleccionstatica.getEstructuras().add(DocumentosC);
						HashMap<String, CompleteDocuments> TablaElem = TablaEquivalDocu.get(Documento);
						if (TablaElem==null)
							TablaElem=new HashMap<String, CompleteDocuments>();
						TablaElem.put(ID,DocumentosC);
						TablaEquivalDocu.put(Documento, TablaElem);
						}
					
				
				}
			rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Genera el metavalue necesario para el proceso
	 * @param completeElementType
	 * @param dato
	 * @return
	 */
	private CompleteElement generaMetavalue(CompleteElementType completeElementType, String dato) {
		if (completeElementType instanceof CompleteTextElementType)
			if (isDate((CompleteTextElementType)completeElementType))
				return generateDate((CompleteTextElementType)completeElementType, dato);
			else return new CompleteTextElement((CompleteTextElementType) completeElementType, dato);
		else
		return null;
	}

	//Test si un elemento es fecha
	private boolean isDate(CompleteTextElementType completeTextElementType) {
		ArrayList<CompleteOperationalValueType> Shows = completeTextElementType.getShows();
		for (CompleteOperationalValueType show : Shows) {	
			if (show.getView().equals(NameConstantsSQL.METATYPE))
			{

					if (show.getName().equals(NameConstantsSQL.METATYPETYPE))
							if (show.getDefault().equals(NameConstantsSQL.DATE)) 
										return true;
			}
		}
		return false;
		
	}



	/**
	 * Genera una fecha
	 * @param meta
	 * @param dato
	 * @return
	 */
	private CompleteElement generateDate(CompleteTextElementType meta, String dato) {		
		
		Date D=setMyCal(meta,dato);
		if (D!=null)
			return new CompleteTextElement(meta, D.toString());
		else return null;
	}

	/**
	 * Setea la fecha
	 * @param meta
	 * @param dato
	 * @return 
	 */
	private Date setMyCal(CompleteTextElementType meta, String dato) {
		
		String DateType=DameTipoDate(meta);
		
		if (DateType!=null)
		{
			if (DateType.equals(Fecha.DATE.toString()))
			{
				SimpleDateFormat formatoDelTexto = new SimpleDateFormat("yyyy-MM-dd");
				Date fecha = null;
				try {
					fecha = formatoDelTexto.parse(dato);
					return fecha;
				} catch (ParseException e) {
					e.printStackTrace();
					return null;
				}
			}
			else if ((DateType.equals(Fecha.DATETIME.toString()))||(DateType.equals(Fecha.TIMESTAMP.toString())))
			{
				SimpleDateFormat formatoDelTexto = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date fecha = null;
				try {
					fecha = formatoDelTexto.parse(dato);
					return fecha;
				} catch (ParseException e) {
					e.printStackTrace();
					return null;
				}
			}
			else if (DateType.equals(Fecha.TIME.toString()))
			{
				SimpleDateFormat formatoDelTexto = new SimpleDateFormat("HH:mm:ss");
				Date fecha = null;
				try {
					fecha = formatoDelTexto.parse(dato);
					return fecha;
				} catch (ParseException e) {
					e.printStackTrace();
					return null;
				}
			}
			else if (DateType.equals(Fecha.YEAR.toString()))
			{
				SimpleDateFormat formatoDelTexto = new SimpleDateFormat("yyyy");
				Date fecha = null;
				try {
					fecha = formatoDelTexto.parse(dato);
					return fecha;
				} catch (ParseException e) {
					e.printStackTrace();
					return null;
				}
			}
			else	
			{
				return null;
			}
		}
		else return null;
		
		
	}

	/**
	 * Devuelve el Show que marca el tipo
	 * @param meta
	 * @return
	 */
	private String DameTipoDate(CompleteTextElementType meta) {
		for (CompleteOperationalValueType show1 : meta.getShows()) {
			if (show1.getView().equals(NameConstantsSQL.SQL))
					if (show1.getName().equals(NameConstantsSQL.TYPECOLUMN))
						return show1.getDefault();
		}
		return null;
	}

	/**
	 * Procesa las columnas de una tabla y sus instancias
	 * @param catalogo
	 * @param tabla
	 * @param padre
	 */
	protected ArrayList<CompleteElementType> procesaColumnas(String catalogo, String tabla, CompleteGrammar padre) {
		ArrayList<CompleteElementType> Salida =new ArrayList<CompleteElementType>();
		try {
			HashMap<String,KeyElement> Keys=new HashMap<String,KeyElement>();
			
			ResultSet rsKey= MySQL.getKey(catalogo, tabla);
			if (rsKey!=null)
				while (rsKey.next())
					{
					String Nombrecolumna = rsKey.getString("COLUMN_NAME");
					String NombreTabla = rsKey.getString("TABLE_NAME");
					String PKName = rsKey.getString("PK_NAME");
					Keys.put(Nombrecolumna,new KeyElement(NombreTabla,Nombrecolumna,PKName));
					
					}
					
			ResultSet rs=MySQL.getColums(catalogo, tabla);
			if (rs!=null) 
			{
				while (rs.next()) {
					   String nombreColumna = rs.getString("COLUMN_NAME");
					   String tipoColumna = rs.getString("TYPE_NAME"); 
					   String numberasoc = rs.getString("COLUMN_SIZE");
					   String isNulable = rs.getString("IS_NULLABLE");
					   String isAutoIcrement =rs.getString("IS_AUTOINCREMENT");
//					   String isGenerated=rs.getString("IS_GENERATEDCOLUMN");
					   
					   KeyElement elem=Keys.get(nombreColumna);
					   String VistaOV=new String(NameConstantsSQL.SQL);
					   
					   
					   CompleteElementType M=generaMeta(nombreColumna,tipoColumna,padre,VistaOV,tabla,numberasoc);
					   
					  
						CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.SQLType,NameConstantsSQL.COLUMNA,VistaOV);
						M.getShows().add(Valor);
						
						CompleteOperationalValueType ValorInt=new CompleteOperationalValueType(NameConstantsSQL.COLUMNIDKEY,Integer.toString(Contadordetablas),VistaOV);
						
						HashMap<String, Integer> Lista = ClaveClaves.get(tabla);
						Integer A=Lista.get(nombreColumna);
						if (A!=null)
							ValorInt=new CompleteOperationalValueType(NameConstantsSQL.COLUMNIDKEY,Integer.toString(A.intValue()),VistaOV);
						else
							{
							Lista.put(nombreColumna, Contadordetablas);
							ClaveClaves.put(tabla, Lista);
							Contadordetablas++;
							}
						
						M.getShows().add(ValorInt);
						
						if (!isNulable.isEmpty())
						{
							CompleteOperationalValueType ValorN=new CompleteOperationalValueType(NameConstantsSQL.ISNULLABE,isNulable,VistaOV);
							M.getShows().add(ValorN);
						}
						
						if (!isAutoIcrement.isEmpty())
						{
							CompleteOperationalValueType ValorN=new CompleteOperationalValueType(NameConstantsSQL.AUTO_INCREMENT,isAutoIcrement,VistaOV);
							M.getShows().add(ValorN);
						}
						
//						if (!isGenerated.isEmpty())
//						{
//							CompleteOperationalValueType ValorN=new CompleteOperationalValueType(NameConstantsSQL.ISGENERATED,isGenerated,VistaOV);
//							M.getShows().add(ValorN);
//						}
						
						
						if (elem!=null)
							{
							CompleteOperationalValueType Valor2=new CompleteOperationalValueType(NameConstantsSQL.KEY,Boolean.toString(true),VistaOV);
							M.getShows().add(Valor2);
							CompleteOperationalValueType Valor3=new CompleteOperationalValueType(NameConstantsSQL.KEYLEVEL,elem.getPKName(),VistaOV);
							M.getShows().add(Valor3);
							if (!tabla.equals(elem.getNombreTabla()))
								{
								CompleteOperationalValueType Valor4=new CompleteOperationalValueType(NameConstantsSQL.FOREINGCOLUMNNAME,elem.getNombreTabla(),VistaOV);
								M.getShows().add(Valor4);
								
								HashMap<String, Integer> Lista2 = ClaveClaves.get(elem.getNombreTabla());
								
								if (Lista2==null)
									Lista2=new HashMap<String, Integer>();
								
								Integer ValorColumnaId = Lista2.get(elem.getNombreColumna());
								if (ValorColumnaId==null)
									{
									ValorColumnaId=new Integer(Contadordetablas);
									Lista2.put(nombreColumna, Contadordetablas);
									ClaveClaves.put(elem.getNombreTabla(), Lista2);
									Contadordetablas++;
									}
								
								
								
								CompleteOperationalValueType Valor5=new CompleteOperationalValueType(NameConstantsSQL.FOREINGCOLUMNIDKEY,Integer.toString(ValorColumnaId),VistaOV);
								M.getShows().add(Valor5);
								}
							}
						
					   
						

						
					   padre.getSons().add(M);
					   Salida.add(M);
					   
					}
			rs.close();
			
			}
			
			
			
			
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		return Salida;
		
	}

	private CompleteElementType generaMeta(String nombreColumna, String tipoColumna, CompleteGrammar padre, String vistaOV,String tabla, String numberasoc) {
		
		
		
		
		//numerico

		
		for (Numbers valornumerico : Numbers.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
			{
				
				
				 CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
				 String VistaOV2=new String(NameConstantsSQL.METATYPE);
				 CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.NUMERIC,VistaOV2);
				 Salida.getShows().add(Valor);

				 
				 CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				 Salida.getShows().add(ValorResult);
					if (numberasoc!=null&&!numberasoc.isEmpty())
						{
						try {
							int I =Integer.parseInt(numberasoc);
							I++;
							CompleteOperationalValueType ValorResult2=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN2,Integer.toString(I),vistaOV);
							Salida.getShows().add(ValorResult2);
						} catch (Exception e) {
							e.printStackTrace();
						}
						
						}
				Salida.setClassOfIterator(Salida);
				 
				return Salida;
			}
		}
		
		for (Fecha valornumerico : Fecha.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{
				
				
				
				CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
				String VistaOV2=new String(NameConstantsSQL.METATYPE);
				CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.DATE,VistaOV2);
				Salida.getShows().add(Valor);
						
				
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				Salida.getShows().add(ValorResult);
				Salida.setClassOfIterator(Salida);
				
				return Salida;
				}
		}
		

		for (Texto valornumerico : Texto.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{

				
				
					CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
					String VistaOV2=new String(NameConstantsSQL.METATYPE);
					CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.TEXT,VistaOV2);
					Salida.getShows().add(Valor);
				 
					
					
					CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
					Salida.getShows().add(ValorResult);
					
					if (numberasoc!=null&&!numberasoc.isEmpty())
					{
					try {
						CompleteOperationalValueType ValorResult2=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN2,numberasoc,vistaOV);
						Salida.getShows().add(ValorResult2);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					}
					
					Salida.setClassOfIterator(Salida);
					
					
					return Salida;
				}
		}
		
		for (Booleanos valornumerico : Booleanos.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{
				
				
				CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
				String VistaOV2=new String(NameConstantsSQL.METATYPE);
				CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.BOOLEAN,VistaOV2);
				Salida.getShows().add(Valor);
				
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				Salida.getShows().add(ValorResult);
				Salida.setClassOfIterator(Salida);
				
				return Salida;
				}
		}
		
		for (Controlado valornumerico : Controlado.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{
				
				CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna, padre);
				
				
				ArrayList<String> V= generaVocabulary(nombreColumna,tabla);
				
				String VistaOV2=new String(NameConstantsSQL.METATYPE);
				CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.CONTROLED,VistaOV2);
				Salida.getShows().add(Valor);
				
				
				String VistaVOC=new String(NameConstantsSQL.VOCABULARY);
				for (String Termino : V) {
					CompleteOperationalValueType ValorTerm=new CompleteOperationalValueType(NameConstantsSQL.TERM,Termino,VistaVOC);
					Salida.getShows().add(ValorTerm);
				}

				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				Salida.getShows().add(ValorResult);
				Salida.setClassOfIterator(Salida);

				return Salida;
				}
		}
		
		CompleteElementType Salida = new CompleteElementType(nombreColumna, padre);
		
		CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,tipoColumna.toString(),vistaOV);
		Salida.getShows().add(ValorResult);
		
		Salida.setClassOfIterator(Salida);
		
		return Salida; 
	}

	/**
	 * Funcion que genera el vocabulario
	 * @param nombreColumna
	 * @param tabla
	 * @return
	 */
	private ArrayList<String> generaVocabulary(String nombreColumna, String tabla) {
		ArrayList<String> Salida=new ArrayList<String>();
		
		try {
			ResultSet rs=MySQL.RunQuerrySELECT("SELECT distinct "+nombreColumna+" FROM "+ tabla +";");
			if (rs!=null) 
			{
				while (rs.next()) {
					
						Object O=rs.getObject(nombreColumna);
						if (O!=null)
							{
							String Dato=O.toString();
							if (Dato!=null&&!Dato.isEmpty())
								{
								Salida.add(Dato);
								}

							}
				}
			rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		return Salida;
	}


	/**
	 * @return the coleccionstatica
	 */
	public  CompleteCollection getColeccion() {
		return coleccionstatica;
	}



	@Override
	public void setMySQLInstance(MySQLConnectionMySQL mySQL) {
		MySQL=mySQL;
		
	}

	@Override
	public void ProcessInstances() {
		//NO ES NECESARIO SE CARGA CON LAS TABLAS
		
	}


	
	
}
