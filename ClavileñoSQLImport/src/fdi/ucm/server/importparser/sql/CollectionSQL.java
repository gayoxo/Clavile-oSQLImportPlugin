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
import fdi.ucm.server.modelComplete.collection.grammar.CompleteOperationalView;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteTextElementType;

/**
 * Clase que implementa la creacion de la base de datos per se
 * @author Joaquin Gayoso-Cabada
 *
 */
public class CollectionSQL implements InterfaceSQLparser {

	private static final String COLECCION_A_APARTIR_DE_UN_SQL = "Coleccion a apartir de un SQL : ";
	private static final String SQL_COLLECTION = "SQL Collection";
	private CompleteCollection coleccionstatica;
	private static enum Numbers {TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, DECIMAL,DEC,FLOAT,DOUBLE};
	private static enum Fecha {DATETIME,
		//'0000-00-00 00:00:00'
							   DATE,
		//'0000-00-00'
							   TIMESTAMP,
		//00000000000000	   
							   TIME,
		//'00:00:00'
							   YEAR
	};
	
	private static enum Texto {CHAR, VARCHAR, BINARY, VARBINARY, BLOB, TEXT, ENUM,LONGTEXT};
	
	private static enum Booleanos {TINYINT,BOOL,BOOLEAN,BIT};
	
	private static enum Controlado {SET};
	

	private MySQLConnectionMySQL MySQL;
	private HashMap<String, HashMap<String, Integer>> ClaveClaves;
	private int Contadordetablas;
	
	public CollectionSQL() {
		coleccionstatica=new CompleteCollection(SQL_COLLECTION, COLECCION_A_APARTIR_DE_UN_SQL+ new Timestamp(new Date().getTime()));
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
						
					   CompleteOperationalView VistaOV=new CompleteOperationalView(NameConstantsSQL.SQL);
						CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.SQLType,NameConstantsSQL.TABLA,VistaOV);
						VistaOV.getValues().add(Valor);
						M.getViews().add(VistaOV);

						
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
	private void procesaColumnasInstancia(String catalogo, String tabla, ArrayList<CompleteElementType> metaColumnas, CompleteGrammar Documento) {
		try {
			ResultSet rs=MySQL.RunQuerrySELECT("SELECT * FROM "+ tabla +";");
			if (rs!=null) 
			{
				while (rs.next()) {
					
					CompleteDocuments DocumentosC=new CompleteDocuments(coleccionstatica,Documento,"Columna","");
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
						coleccionstatica.getEstructuras().add(DocumentosC);
					
				
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
		ArrayList<CompleteOperationalView> Shows = completeTextElementType.getShows();
		for (CompleteOperationalView show : Shows) {	
			if (show.getName().equals(NameConstantsSQL.METATYPE))
			{
				ArrayList<CompleteOperationalValueType> ShowValue = show.getValues();
				for (CompleteOperationalValueType showValues : ShowValue) {
					if (showValues.getName().equals(NameConstantsSQL.METATYPETYPE))
							if (showValues.getDefault().equals(NameConstantsSQL.DATE)) 
										return true;
				}
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
		for (CompleteOperationalView show1 : meta.getShows()) {
			if (show1.getName().equals(NameConstantsSQL.SQL))
				for (CompleteOperationalValueType show1value : show1.getValues()) {
					if (show1value.getName().equals(NameConstantsSQL.TYPECOLUMN))
						return show1value.getDefault();
				}
		}
		return null;
	}

	/**
	 * Procesa las columnas de una tabla y sus instancias
	 * @param catalogo
	 * @param tabla
	 * @param padre
	 */
	private ArrayList<CompleteElementType> procesaColumnas(String catalogo, String tabla, CompleteGrammar padre) {
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
					   String nombreColumna = rs.getString(4);
					   String tipoColumna = rs.getString(6); 
					   String numberasoc = rs.getString(7); 
					   
					   KeyElement elem=Keys.get(nombreColumna);
					   
					   CompleteOperationalView VistaOV=new CompleteOperationalView(NameConstantsSQL.SQL);
						CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.SQLType,NameConstantsSQL.COLUMNA,VistaOV);
						VistaOV.getValues().add(Valor);
						
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
						
						VistaOV.getValues().add(ValorInt);
						
						
						
						//TODO AQUI PAsAN COSAS
						
						if (elem!=null)
							{
							CompleteOperationalValueType Valor2=new CompleteOperationalValueType(NameConstantsSQL.KEY,Boolean.toString(true),VistaOV);
							VistaOV.getValues().add(Valor2);
							CompleteOperationalValueType Valor3=new CompleteOperationalValueType(NameConstantsSQL.KEYLEVEL,elem.getPKName(),VistaOV);
							VistaOV.getValues().add(Valor3);
							if (!tabla.equals(elem.getNombreTabla()))
								{
								CompleteOperationalValueType Valor4=new CompleteOperationalValueType(NameConstantsSQL.FOREINGCOLUMNNAME,elem.getNombreTabla(),VistaOV);
								VistaOV.getValues().add(Valor4);
								
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
								VistaOV.getValues().add(Valor5);
								}
							}
						
					   CompleteElementType M=generaMeta(nombreColumna,tipoColumna,padre,VistaOV,tabla,numberasoc);
						
						
						M.getShows().add(VistaOV);
						
					   padre.getSons().add(M);
					   Salida.add(M);
					   
					}
			rs.close();
			
			}
			
			
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return Salida;
		
	}

	private CompleteElementType generaMeta(String nombreColumna, String tipoColumna, CompleteGrammar padre, CompleteOperationalView vistaOV,String tabla, String numberasoc) {
		
		
		
		
		//numerico

		
		for (Numbers valornumerico : Numbers.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
			{
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				vistaOV.getValues().add(ValorResult);
				if (numberasoc!=null&&!numberasoc.isEmpty())
					{
					try {
						int I =Integer.parseInt(numberasoc);
						I++;
						CompleteOperationalValueType ValorResult2=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN2,Integer.toString(I),vistaOV);
						vistaOV.getValues().add(ValorResult2);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					}
				
				 CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
				 CompleteOperationalView VistaOV2=new CompleteOperationalView(NameConstantsSQL.METATYPE);
				 CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.NUMERIC,VistaOV2);
				 VistaOV2.getValues().add(Valor);
				 Salida.getShows().add(VistaOV2);
				 
				return Salida;
			}
		}
		
		for (Fecha valornumerico : Fecha.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				vistaOV.getValues().add(ValorResult);
				
				
				CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
				CompleteOperationalView VistaOV2=new CompleteOperationalView(NameConstantsSQL.METATYPE);
				CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.DATE,VistaOV2);
				VistaOV2.getValues().add(Valor);
				Salida.getShows().add(VistaOV2);
				
				return Salida;
				}
		}
		

		for (Texto valornumerico : Texto.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{

				
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				vistaOV.getValues().add(ValorResult);
				
				if (numberasoc!=null&&!numberasoc.isEmpty())
				{
				try {
					CompleteOperationalValueType ValorResult2=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN2,numberasoc,vistaOV);
					vistaOV.getValues().add(ValorResult2);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				}
					CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
					CompleteOperationalView VistaOV2=new CompleteOperationalView(NameConstantsSQL.METATYPE);
					CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.TEXT,VistaOV2);
					VistaOV2.getValues().add(Valor);
					Salida.getShows().add(VistaOV2);
				 
					return Salida;
				}
		}
		
		for (Booleanos valornumerico : Booleanos.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				vistaOV.getValues().add(ValorResult);
				
				CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna,padre); 
				
				CompleteOperationalView VistaOV2=new CompleteOperationalView(NameConstantsSQL.METATYPE);
				CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.BOOLEAN,VistaOV2);
				VistaOV2.getValues().add(Valor);
				Salida.getShows().add(VistaOV2);
				
				return Salida;
				}
		}
		
		for (Controlado valornumerico : Controlado.values()) {
			if (tipoColumna.startsWith(valornumerico.toString()))
				{
				CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,valornumerico.toString(),vistaOV);
				vistaOV.getValues().add(ValorResult);
				CompleteTextElementType Salida=new CompleteTextElementType(nombreColumna, padre);
				
				
				ArrayList<String> V= generaVocabulary(nombreColumna,tabla);
				
				CompleteOperationalView VistaOV2=new CompleteOperationalView(NameConstantsSQL.METATYPE);
				CompleteOperationalValueType Valor=new CompleteOperationalValueType(NameConstantsSQL.METATYPETYPE,NameConstantsSQL.CONTROLED,VistaOV2);
				VistaOV2.getValues().add(Valor);
				Salida.getShows().add(VistaOV2);
				
				
				CompleteOperationalView VistaVOC=new CompleteOperationalView(NameConstantsSQL.VOCABULARY);
				for (String Termino : V) {
					CompleteOperationalValueType ValorTerm=new CompleteOperationalValueType(NameConstantsSQL.TERM,Termino,VistaVOC);
					VistaVOC.getValues().add(ValorTerm);
				}
				Salida.getShows().add(VistaVOC);
				

				return Salida;
				}
		}
		
		CompleteOperationalValueType ValorResult=new CompleteOperationalValueType(NameConstantsSQL.TYPECOLUMN,tipoColumna.toString(),vistaOV);
		vistaOV.getValues().add(ValorResult);
		return new CompleteElementType(nombreColumna, padre);
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
