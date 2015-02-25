package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.sun.jmx.snmp.Timestamp;

import cn.edu.sjtu.acm.jdbctaste.dao.JokeDao;
import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public class SqliteJokeDao implements JokeDao {

	public static final int IDX_ID = 1, IDX_BODY = 2, IDX_SPEAKER = 3,
			IDX_POST_TIME = 4, IDX_ZAN = 5;

	private final Connection conn;

	public SqliteJokeDao(Connection conn) {
		this.conn = conn;
	}

	@Override
	public int insertJoke(Joke joke) {
		//cqm
		int ret = -1;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"insert into joke (body, speaker, posttime, zan) values (?,?,?,?);",
					Statement.RETURN_GENERATED_KEYS);
			stat.setInt(2, joke.getSpeaker().getId());
			stat.setString(1, joke.getBody());
			stat.setTimestamp(3, joke.getPostTime());
			stat.setInt(4, joke.getZan());
			
			stat.executeUpdate();
			
			ResultSet rs = stat.getGeneratedKeys();
			
			if (rs.next()) {
				int id = rs.getInt(1);
				joke.setId(id);
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
	public boolean deleteJoke(Joke joke) {
		// cqm
		boolean ret = false;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"delete from joke where id = ?;");
			stat.setInt(1, joke.getId());

			stat.executeUpdate();
			boolean flag = true;
			
			List<Comment> commentList = SqliteDaoFactory.getInstance().getCommentDao().findCommentsOfJoke(joke);
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
	public boolean updateJoke(Joke joke) {
		// cqm
		boolean ret = false;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"update joke set body = ?, speaker = ?, posttime = ?, zan = ? where id = ?;");
			stat.setInt(2, joke.getSpeaker().getId());
			stat.setString(1, joke.getBody());
			stat.setTimestamp(3, joke.getPostTime());
			stat.setInt(4, joke.getZan());
			stat.setInt(5, joke.getId());
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
	public List<Joke> findJokesOfPerson(Person person) {
		// cqm
		List<Joke> ret = new LinkedList<Joke>();
		
		try {
			PreparedStatement stat = conn
					.prepareStatement("select * from joke where speaker = ?;");
			stat.setInt(1, person.getId());
			ResultSet result = stat.executeQuery();

			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			Person speaker;
			
			while (result.next()) 
			{
				speaker = personDao.findPersonById(result.getInt(IDX_SPEAKER));
				ret.add(new Joke(result.getInt(IDX_ID), speaker, result.getString(IDX_BODY), 
						result.getTimestamp(IDX_POST_TIME), result.getInt(IDX_ZAN)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public List<Joke> getAllJokes() {
		// cqm
		List<Joke> ret = new LinkedList<Joke>();

		Statement stat;
		try {
			stat = conn.createStatement();

			stat.execute("select * from joke;");
			ResultSet result = stat.getResultSet();
			
			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			Person speaker;
			
			while (result.next()) 
			{
				speaker = personDao.findPersonById(result.getInt(IDX_SPEAKER));
				ret.add(new Joke(result.getInt(IDX_ID), speaker, result.getString(IDX_BODY), 
						result.getTimestamp(IDX_POST_TIME), result.getInt(IDX_ZAN)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public Joke findJokeById(int id) {
		Joke ret = null;

		try {
			PreparedStatement stat = conn
					.prepareStatement("select * from joke where id = ?;");
			stat.setInt(1, id);
			ResultSet result = stat.executeQuery();
			if (!result.next())
				return null;

			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			Person speaker = personDao.findPersonById(result.getInt(IDX_SPEAKER));
			
			ret = new Joke(result.getInt(IDX_ID), speaker,
					result.getString(IDX_BODY),
					result.getTimestamp(IDX_POST_TIME), result.getInt(IDX_ZAN));
			
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public List<Joke> findJokesWithZanMoreThan(int zan) {
		// cqm
		List<Joke> ret = new LinkedList<Joke>();

		try {
			PreparedStatement stat = conn
					.prepareStatement("select * from joke where zan > ?;");
			stat.setInt(1, zan);
			ResultSet result = stat.executeQuery();

			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			Person speaker;
			
			while (result.next()) 
			{
				speaker = personDao.findPersonById(result.getInt(IDX_SPEAKER));
				ret.add(new Joke(result.getInt(IDX_ID), speaker, result.getString(IDX_BODY), 
						result.getTimestamp(IDX_POST_TIME), result.getInt(IDX_ZAN)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}
}
