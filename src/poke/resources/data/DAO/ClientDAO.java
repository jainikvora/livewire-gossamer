package poke.resources.data.DAO;

import java.sql.*;

public class ClientDAO {
	private Connection connection = null;
	private Statement statement = null;
	private Statement statement1 = null;
	private ResultSet resultset = null;
	private ResultSet resultset1 = null;
	private PreparedStatement preparedStatement = null;

	public long updateClientEntry(String NodeID, String ClientID, long SentIndex) {

		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(
					"jdbc:mysql://10.0.1.2/cmpe275", "root", "root");
			statement = connection.createStatement();
			statement1 = connection.createStatement();
			resultset = statement
					.executeQuery("Select * from clientdetails where ClientID = "
							+ "'" + ClientID + "'");

			if (!resultset.next()) {
				preparedStatement = connection
						.prepareStatement("insert into  ClientDetails(NodeID, ClientID, SentIndex) values (?, ?, ?)");

				preparedStatement.setString(1, NodeID);
				preparedStatement.setString(2, ClientID);
				preparedStatement.setLong(3, SentIndex);
				
				System.out.println(preparedStatement.executeUpdate());

				return SentIndex;

			}
			else {
				System.out
						.println("Select * from clientdetails where ClientID = "
								+ "'" + ClientID + "'");
				resultset1 = statement1
						.executeQuery("Select * from clientdetails where ClientID = "
								+ "'" + ClientID + "'");
				resultset1.next();
				System.out.println(resultset1.getString("NodeID") + " == " + NodeID + ": "+ resultset1.getString("NodeID")
						.equals(NodeID));
				if (!(resultset1.getString("NodeID").equals(NodeID))) { //ServerID is different, then update the ServerID for that Client and set new ServerID
					preparedStatement = connection
							.prepareStatement("UPDATE clientdetails SET NodeID = "
									+ "'"
									+ NodeID
									+ " ' WHERE ClientID = "
									+ "'" + ClientID + "'");
					// System.out.println(preparedStatement.executeUpdate());
					return resultset1.getLong("SentIndex");
				}

				else {
					// System.out.println("Called");
					// System.out.println("UPDATE clientdetails SET SentIndex = "+"'"+SentIndex+"'"
					// +"WHERE ClientID = "+"'"+ClientID+"'");
					preparedStatement = connection
							.prepareStatement("UPDATE clientdetails SET SentIndex = "
									+ "'"
									+ SentIndex
									+ "'"
									+ "WHERE ClientID = "
									+ "'"
									+ ClientID
									+ "'");
					preparedStatement.executeUpdate();

					return SentIndex;

				}

			}

		}

		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return SentIndex;

	}

}
