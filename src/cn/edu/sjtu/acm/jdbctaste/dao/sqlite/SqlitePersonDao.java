package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import com.sun.xml.internal.ws.api.pipe.NextAction;

import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public class SqlitePersonDao implements PersonDao {

	public static final int IDX_ID = 1, IDX_NAME = 2, IDX_EMAIL = 3;

	private final Connection conn;

	public SqlitePersonDao(Connection conn) {
		this.conn = conn;
	}

	@Override
	public int insertPerson(Person person) {

		int ret = -1;

		try {
			PreparedStatement stat = conn.prepareStatement(
					"insert into person (name, email) values (?,?);",
					Statement.RETURN_GENERATED_KEYS);
			stat.setString(1, person.getName());
			stat.setString(2, person.getEmail());

			stat.executeUpdate();
			
			ResultSet rs = stat.getGeneratedKeys();
			
			if (rs.next()) {
				int id = rs.getInt(1);
				person.setId(id);
				ret = id;
			}
			
			rs.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = -1;
		}

		return ret;
	}

	@Override
	public boolean deletePerson(Person person) {
		// cqm
		boolean ret = false;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"delete from person where id = ?;");
			stat.setInt(1, person.getId());

			stat.executeUpdate();
			boolean flag = true;
			
			List<Joke> jokeList = SqliteDaoFactory.getInstance().getJokeDao().findJokesOfPerson(person);
			for (Joke joke : jokeList)
			{
				if (SqliteDaoFactory.getInstance().getJokeDao().deleteJoke(joke) == false)
					flag = false;
			}
			
			List<Comment> commentList = SqliteDaoFactory.getInstance().getCommentDao().findCommentsOfPerson(person);
			for (Comment comment : commentList)
			{
				if (SqliteDaoFactory.getInstance().getCommentDao().deleteComment(comment) == false)
					flag = false;
			}
			
			if (flag == true)
				ret = true;
			
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = false;
		}
		return ret;
	}

	@Override
	public boolean updatePerson(Person person) {
		// cqm
		boolean ret = false;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"update person set name = ?, email = ? where id = ?;");
			stat.setString(1, person.getName());
			stat.setString(2, person.getEmail());
			stat.setInt(3, person.getId());
			int count = stat.executeUpdate();
			if (count != 0)
				ret = true;
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = false;
		}
		return ret;
	}

	@Override
	public Person findPersonByEmail(String email) {
		// cqm
		Person ret = null;
		
		PreparedStatement stat;
		try
		{
			stat = conn.prepareStatement("select * from person where email = ?;");
			stat.setString(1, email);
			ResultSet rs = stat.executeQuery();
			
			if (!rs.next())
				return null;
			
			ret = new Person(rs.getInt(IDX_ID), rs.getString(IDX_NAME), rs.getString(IDX_EMAIL));
			
			rs.close();
			stat.close();
		
		} catch (SQLException e)
		{
			e.printStackTrace();
			return null;
		}
		return ret;
	}

	@Override
	public int getNumOfJokes(Person person) {
		// cqm
		int ret = -1;
		
		PreparedStatement stat;
		try
		{
			stat = conn.prepareStatement("select count(*) from Joke where speaker = ?");
			stat.setInt(1, person.getId());
			ResultSet result = stat.executeQuery();
			
			if (!result.next())
				return 0;
			
			ret = result.getInt(1);
			
			result.close();
			stat.close();
		
		} catch (SQLException e)
		{
			e.printStackTrace();
			return -1;
		}
		return ret;
	}

	@Override
	public List<Person> getAllPerson() {
		List<Person> ret = new LinkedList<Person>();

		Statement stat;
		try {
			stat = conn.createStatement();

			stat.execute("select * from person;");
			ResultSet result = stat.getResultSet();

			while (result.next()) {
				ret.add(new Person(result.getInt(IDX_ID), result
						.getString(IDX_NAME), result.getString(IDX_EMAIL)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public Person findPersonById(int id) {
		// cqm
		Person ret = null;
		
		PreparedStatement stat;
		try
		{
			stat = conn.prepareStatement("select * from person where id = ?;");
			stat.setInt(1,id);
			ResultSet rs = stat.executeQuery();
			
			if (!rs.next())
				return null;
			
			ret = new Person(rs.getInt(IDX_ID), rs.getString(IDX_NAME), rs.getString(IDX_EMAIL));
			
			rs.close();
			stat.close();
		
		} catch (SQLException e)
		{
			e.printStackTrace();
			return null;
		}
		return ret;
	}

}
