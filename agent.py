import os
import duckdb
import json
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain.agents import create_openai_tools_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

load_dotenv()
if not os.getenv("OPENAI_API_KEY"):
    raise ValueError("OPENAI_API_KEY not found in .env file")

DB_FILE = 'samarth.db'


@tool
def get_top_crops_by_production(state: str, year: int, top_m: int) -> str:
    """
    Finds the top M most produced crops (by volume in tonnes) in a given state 
    for a specific year.
    """
    print(f"Tool called: get_top_crops_by_production(state={state}, year={year}, top_m={top_m})")
    try:
        con = duckdb.connect(database=DB_FILE, read_only=True)
        query = f"""
        SELECT 
            crop, 
            CAST(SUM(CAST(production_tonnes AS DOUBLE)) AS INTEGER) AS total_production,
            ANY_VALUE(source_url) AS source_url
        FROM agriculture_production
        WHERE state ILIKE '{state}' AND CAST(year AS INTEGER) = {year}
        GROUP BY crop
        ORDER BY total_production DESC
        LIMIT {top_m};
        """
        result = con.execute(query).df()
        con.close()
        
        if result.empty:
            return f"No production data found for {state} in {year}."
            
        return result.to_json(orient="records")
    except Exception as e:
        return f"An error occurred while running the database query: {e}. Check state name spelling or data availability."
@tool
def get_average_annual_rainfall(subdivision: str, start_year: int, end_year: int) -> str:
    """
    Calculates the average annual rainfall (in mm) for a given meteorological 
    subdivision over a range of years.
    """
    print(f"Tool called: get_average_annual_rainfall(subdivision={subdivision}, start_year={start_year}, end_year={end_year})")
    try:
        con = duckdb.connect(database=DB_FILE, read_only=True)
        query = f"""
        WITH YearlyTotals AS (
            SELECT 
                year, 
                SUM(rainfall_mm) AS total_annual_rainfall,
                ANY_VALUE(source_url) AS source_url
            FROM climate_rainfall
            WHERE subdivision ILIKE '{subdivision}' 
              AND year BETWEEN {start_year} AND {end_year}
            GROUP BY year
        )
        SELECT 
            AVG(total_annual_rainfall) AS average_annual_rainfall,
            ANY_VALUE(source_url) AS source_url
        FROM YearlyTotals;
        """
        result = con.execute(query).df()
        con.close()
        return result.to_json(orient="records")
    except Exception as e:
        return f"Error executing query: {e}"

@tool
def correlate_crop_and_climate(
    crop_name: str, 
    state: str, 
    subdivision: str, 
    start_year: int, 
    end_year: int
) -> str:
    """
    Analyzes the production trend of a specific crop in a state and correlates 
    it with the annual rainfall trend in the corresponding climate subdivision 
    over the same period.
    """
    print(f"Tool called: correlate_crop_and_climate(crop={crop_name}, state={state}, subdivision={subdivision}, ...)")
    try:
        con = duckdb.connect(database=DB_FILE, read_only=True)
        
        crop_query = f"""
        SELECT 
            year, 
            SUM(production_tonnes) AS total_production,
            ANY_VALUE(source_url) AS source_url
        FROM agriculture_production
        WHERE state ILIKE '{state}' 
          AND crop ILIKE '{crop_name}'
          AND year BETWEEN {start_year} AND {end_year}
        GROUP BY year
        ORDER BY year;
        """
        crop_df = con.execute(crop_query).df()
        
        climate_query = f"""
        SELECT 
            year, 
            SUM(rainfall_mm) AS total_annual_rainfall,
            ANY_VALUE(source_url) AS source_url
        FROM climate_rainfall
        WHERE subdivision ILIKE '{subdivision}' 
          AND year BETWEEN {start_year} AND {end_year}
        GROUP BY year
        ORDER BY year;
        """
        climate_df = con.execute(climate_query).df()
        
        con.close()
        
        correlation_data = {
            "crop_production_trend": crop_df.to_dict(orient="records"),
            "rainfall_trend": climate_df.to_dict(orient="records")
        }
        return json.dumps(correlation_data, indent=2)
        
    except Exception as e:
        return f"Error executing query: {e}"


def create_agent_executor():
    """Creates the LangChain agent and executor."""
    
    tools = [
        get_top_crops_by_production, 
        get_average_annual_rainfall,
        correlate_crop_and_climate
    ]
    
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    
    prompt_template = """
    You are Project Samarth, a specialized policy and agriculture assistant.
    Your mission is to answer complex questions by synthesizing data from 
    Indian government sources.
    
    - You MUST use your tools to find data. Do not make up answers.
    - The agriculture data has state names. The climate data has 
      'subdivision' names. You must map them correctly. For example, 
      the 'Punjab' state is in the 'Punjab' subdivision. 'Maharashtra' 
      is complex and maps to multiple subdivisions like 'Vidarbha' or 
      'Marathwada'. Use your best judgment or ask the user for clarification 
      if a state maps to many subdivisions.
    - When you get results from a tool, the data will be in JSON format.
    - Synthesize all the JSON data into a single, coherent, easy-to-read 
      answer for the user.
    - **CRITICAL: You MUST cite your sources.** The `source_url` field 
      is included in every tool's JSON output. After any data point, add a 
      citation like this: (Source: <source_url>).
    """
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", prompt_template),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])
    
    agent = create_openai_tools_agent(llm, tools, prompt)
    
    agent_executor = AgentExecutor(
        agent=agent, 
        tools=tools, 
        verbose=True 
    )
    
    return agent_executor

if __name__ == "__main__":
    print("Testing agent setup...")
    agent_executor = create_agent_executor()
    
    chat_history = []
    
    print("Agent is ready. Ask a question (e.g., 'What were the top 3 crops in Punjab in 2010?')")
    
    while True:
        user_input = input("You: ")
        if user_input.lower() in ['exit', 'quit']:
            break
            
        result = agent_executor.invoke({
            "input": user_input,
            "chat_history": chat_history
        })
        
        print(f"Samarth: {result['output']}")
        
        chat_history.append(("human", user_input))
        chat_history.append(("ai", result['output']))