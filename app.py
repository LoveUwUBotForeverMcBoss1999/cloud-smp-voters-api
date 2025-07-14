from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import os
import json
import re
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database configuration
DB_CONFIG = {
    'host': 'de17.spaceify.eu',
    'port': 3306,
    'user': 'u31736_4YAaThHXdg',
    'password': 'LSd5Xo77Ve=WK=ZyhvOQ.TUh',
    'database': 's31736_a_db_baby',
    'charset': 'utf8mb4'
}


def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def parse_last_votes(last_votes_str):
    """Parse the LastVotes field which appears to be serialized data"""
    if not last_votes_str:
        return None, None

    try:
        # Try to parse as JSON first
        parsed_data = json.loads(last_votes_str)
        return parsed_data, None
    except:
        # If JSON parsing fails, try to extract timestamp from string format
        # Format appears to be: "MinecraftMP//1752507579111"
        timestamp_match = re.search(r'//(\d+)', last_votes_str)
        if timestamp_match:
            timestamp = int(timestamp_match.group(1))
            # Convert milliseconds to seconds
            timestamp_seconds = timestamp / 1000
            readable_time = datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')

            # Remove the timestamp part from LastVotes, keep only the site name
            clean_last_votes = re.sub(r'//\d+', '', last_votes_str)
            return clean_last_votes, readable_time

        return last_votes_str, None


@app.route('/api/voter/<int:rank>', methods=['GET'])
def get_voter_by_rank(rank):
    """Get voter by rank (1 = most voted, 2 = second most voted, etc.)"""

    if rank < 1:
        return jsonify({'error': 'Rank must be 1 or greater'}), 400

    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Query to get voters ordered by AllTimeTotal descending
        query = """
        SELECT PlayerName, LastVotes, AllTimeTotal 
        FROM VotingPlugin_Users 
        WHERE PlayerName IS NOT NULL 
        AND AllTimeTotal > 0
        ORDER BY AllTimeTotal DESC 
        LIMIT %s OFFSET %s
        """

        cursor.execute(query, (1, rank - 1))
        result = cursor.fetchone()

        if not result:
            return jsonify({'error': f'No voter found at rank {rank}'}), 404

        # Parse LastVotes field
        last_votes, parsed_time = parse_last_votes(result['LastVotes'])

        response = {
            'rank': rank,
            'PlayerName': result['PlayerName'],
            'LastVotes': last_votes,
            'AllTimeTotal': result['AllTimeTotal']
        }

        # Add Time field if timestamp was found and parsed
        if parsed_time:
            response['Time'] = parsed_time

        return jsonify(response)

    except Error as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


@app.route('/api/voters/top/<int:limit>', methods=['GET'])
def get_top_voters(limit):
    """Get top N voters (bonus endpoint)"""

    if limit < 1 or limit > 100:
        return jsonify({'error': 'Limit must be between 1 and 100'}), 400

    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        query = """
        SELECT PlayerName, LastVotes, AllTimeTotal 
        FROM VotingPlugin_Users 
        WHERE PlayerName IS NOT NULL 
        AND AllTimeTotal > 0
        ORDER BY AllTimeTotal DESC 
        LIMIT %s
        """

        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        voters = []
        for i, result in enumerate(results, 1):
            last_votes, parsed_time = parse_last_votes(result['LastVotes'])

            voter = {
                'rank': i,
                'PlayerName': result['PlayerName'],
                'LastVotes': last_votes,
                'AllTimeTotal': result['AllTimeTotal']
            }

            # Add Time field if timestamp was found and parsed
            if parsed_time:
                voter['Time'] = parsed_time

            voters.append(voter)

        return jsonify({
            'total_results': len(voters),
            'voters': voters
        })

    except Error as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    connection = get_db_connection()
    if connection:
        connection.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    else:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    app.run(debug=True)