
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

/**
 * EasyMongo JavaLIB para aplicações simples.
 * @author Katho
 *
 */

public class MongoManager {

	public MongoClient client;
	public DB db;
	private boolean authorized;
	
	/**
	 * Constructor de conexão simples.
	 * @param host Endereço do servidor aonde esta localizado o mongodb.
	 * @param port Porta aonde o mongodb esta listando.
	 * @param database Databse que a lib ira se conectar.
	 */
	public MongoManager(String host, int port, String database) {
		
		try {
			
			this.client = new MongoClient(host, port);
			this.db = client.getDB(database);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Constructor de conexão autorizada.
	 * @param host Endereço do servidor aonde esta localizado o mongodb.
	 * @param port Porta aonde o mongodb esta listando.
	 * @param database Databse que a lib ira se conectar.
	 * @param login Usuario que ira se requisitar login.
	 * @param Senha do usuario.
	 */
	public MongoManager(String host, int port, String database, String login, String pwd) {
		
		try {
			
			this.client = new MongoClient(host, port);
			this.db = client.getDB(database);
			
			this.authorized = db.authenticate(login, pwd.toCharArray());
			
			if (this.authorized == false) {
				throw new Exception("Não autorizado.");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Cria uma coleção no servidor.
	 * @param collectionName Nome da coleção que ira ser criada.
	 */
	public void createCollection(String collectionName) {
		this.db.createCollection(collectionName, null);
	}
	
	/**
	 * Retorna uma coleção do servidor.
	 * @param collectionName Nome da coleção que será retornada.
	 */
	public DBCollection getCollection(String collectionName) {
		DBCollection col = this.db.getCollection(collectionName);
		return col;
	}
	
	/**
	 * Retorna o find() de uma collection.
	 * @param col Coleção daonde será feito o find.
	 */
	public String find(DBCollection col) {
		col = getCollection(col.getName());
		
		DBCursor cursor = col.find();
		StringBuilder builder = new StringBuilder();
		
		while(cursor.hasNext()) {
			builder.append(cursor.next());
		}
		
		return builder.toString();
		
	}
	
	/**
	 * Retorna o find() com uma key e um value como filtros.
	 * @param col Coleção daonde será feito o find.
	 * @param key Key do objeto que será pesquisado.
	 * @param value Value que quer comparar para ser retornado.
	 */
	public String find(DBCollection col, String key, String value) {
		
		col = getCollection(col.getName());
		
		BasicDBObject obj = new BasicDBObject(key , value);
		
		DBCursor cursor = col.find(obj);
		StringBuilder builder = new StringBuilder();
		
		while (cursor.hasNext()) {
			builder.append(cursor.next());
		}

		return builder.toString();
		
	}
	
	/**
	 * Insere um objeto em uma coleção.
	 * @param col Coleção donde será inserido.
	 * @param object Objeto que será inserido.
	 */
	public void insert(DBCollection col, String object) {
		
		col = getCollection(col.getName());
		
		DBObject obj = (DBObject) JSON.parse(object);
		col.insert(obj);
		
	}
	
	
	/**
	 * Atualiza os valores de um objeto pela key(Chave);
	 * @param col Coleção aonde será atualizado.
	 * @param key Chave do valor que será atualizado
	 * @param lastValue Valor antigo que será, pego e atualizado.
	 * @param newValue Novo valor que será agregado a chave.
	 */
	public void update(DBCollection col, String key, String lastValue, String newValue) {
		
		col = getCollection(col.getName());
		
		BasicDBObject obj = new BasicDBObject(key, lastValue);
		
		DBCursor cursor = col.find(obj);
		
		while (cursor.hasNext()) {
			DBObject update = cursor.next();
			update.put(key, newValue);
			col.update(obj, update);
		}
		
	}
	
	/**
	 * Deleta um objeto.
	 * @param col Colection quer será atualizada.
	 * @param key Key do valor do objeto.
	 * @param value Valor que será comparado para exlusão do objeto.
	 */
	public void delete(DBCollection col, String key, String value) {
		
		
		
	}
	
	

}
