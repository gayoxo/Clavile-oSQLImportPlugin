/**
 * 
 */
package fdi.ucm.server.importparser.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import fdi.ucm.server.modelComplete.collection.document.CompleteDocuments;
import fdi.ucm.server.modelComplete.collection.document.CompleteLinkElement;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteElementType;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteGrammar;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteLinkElementType;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteOperationalValueType;
import fdi.ucm.server.modelComplete.collection.grammar.CompleteOperationalView;

/**
 * @author Joaquin Gayoso-Cabada
 *
 */
public class CollectionSQLInfering extends CollectionSQL {

	
	private class TablaPar{
		
		private String Tabla;
		private String Coleccion;
		
		
		
		public TablaPar(String tabla, String coleccion) {
			super();
			Tabla = tabla;
			Coleccion = coleccion;
		}
		public String getTabla() {
			return Tabla;
		}

		public String getColeccion() {
			return Coleccion;
		}

		
		
		
	}

	public CollectionSQLInfering() {
		super();
	}

	
	@Override
	public void ProcessAttributes() {
		try {
			
			
			//REhacerlo con un bucle enlazado.
			
			
			
			HashMap<String, CompleteGrammar> GramaticasBase = new HashMap<String, CompleteGrammar>();
			
			ArrayList<TablaPar> Tablas=new ArrayList<CollectionSQLInfering.TablaPar>();

			ResultSet rs=MySQL.getTables();
			if (rs!=null) 
			{
				while (rs.next()) {
					String catalogo = rs.getString(1);
					String tabla = rs.getString(3);
					Tablas.add(new TablaPar(tabla, catalogo));
				}
				rs.close();
			}
			
			
			
			
			
			
			ClaveClaves=new HashMap<String, HashMap<String, Integer>>();
			Contadordetablas=0;
			HashSet<String> Procesadas = new HashSet<String>();
			
			boolean continueW=true;
			int posi=0;
			
			while (continueW)
			{
			continueW=false;
			
			for (TablaPar tablaPar : Tablas) {
				String catalogo = tablaPar.getColeccion();
				String tabla = tablaPar.getTabla();
				String[] palabras=tabla.split("_");
				if (palabras.length==posi+1&&!Procesadas.contains(tabla))
					{
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
					   
					   GramaticasBase.put(tabla, M);
					   Procesadas.contains(tabla);
					   continueW=true;
					}
			}
			
			for (TablaPar tablaPar : Tablas) {
				String tabla = tablaPar.getTabla();
				String[] palabras=tabla.split("_");
				if (palabras.length>posi+1&&!Procesadas.contains(tabla))
					{
					StringBuffer nuevaString=new StringBuffer();
					for (int i = 0; i < posi+1; i++) {
						if (nuevaString.length()>0)
							nuevaString.append("_");
						nuevaString.append(palabras[i]);
						
					}
					String Source = nuevaString.toString();
					
					StringBuffer nuevaString2=new StringBuffer();
					for (int i = posi+1; i < palabras.length; i++) {
						if (nuevaString2.length()>0)
							nuevaString2.append("_");
						nuevaString2.append(palabras[i]);
					}
					String Target = nuevaString2.toString();
					
					CompleteGrammar SourceG= GramaticasBase.get(Source);
					
					if (SourceG==null)
						SourceG= GramaticasBase.get(Source+"s");
					
					if (SourceG==null)
						SourceG= GramaticasBase.get(Source+"es");
					
					CompleteGrammar TargetG= GramaticasBase.get(Target);
					
					if (TargetG==null)
						TargetG= GramaticasBase.get(Target+"s");
					
					if (TargetG==null)
						TargetG= GramaticasBase.get(Target+"es");
					
					if (SourceG!=null&&TargetG!=null)
					{
						
					CompleteLinkElementType Relacion=new CompleteLinkElementType(Target, SourceG);
					SourceG.getSons().add(Relacion);
					
					
					procesaColumnasInstancia(tablaPar.getTabla(),SourceG,TargetG,Relacion);
					Procesadas.add(tabla);
					}
				}
			
			}
			
			posi++;
			
			
			}
						
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	private void procesaColumnasInstancia(String tabla,
			CompleteGrammar sourceG, CompleteGrammar targetG,
			CompleteLinkElementType relacion) {
		try {
			ResultSet rs=MySQL.RunQuerrySELECT("SELECT * FROM "+ tabla +";");
			if (rs!=null) 
			{
				while (rs.next()) {
					
					try {
						String SourceD = rs.getString(1);
						String TargetD = rs.getString(2);
						
						if (SourceD!=null&&TargetD!=null)	
						{
							HashMap<String, CompleteDocuments> CDST=TablaEquivalDocu.get(sourceG);
							HashMap<String, CompleteDocuments> CDTT=TablaEquivalDocu.get(targetG);
							if (CDST!=null&&CDTT!=null)
								{
								CompleteDocuments CDS = CDST.get(SourceD);
								CompleteDocuments CDT = CDTT.get(TargetD);
									if (CDS!=null&&CDT!=null)
									{
										CompleteLinkElement CLE=new CompleteLinkElement(relacion, CDT);
										CDS.getDescription().add(CLE);
									}
								}
						}
						
					} catch (Exception e) {
						e.printStackTrace();
						//TODO error en linea
						
					}
					
						
										
				
				}
			rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		
	}
}
