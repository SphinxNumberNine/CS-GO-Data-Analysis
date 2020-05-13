import pandas as pd
import numpy as np
from datetime import date, timedelta
import datetime
from bs4 import BeautifulSoup
import requests
import re
import math
import os
import _pickle as pickle


# ----------------------------------------------------- HISTORICAL RANKING DATA -----------------------------------------------------

months = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december"
}

ranking_pickle_filepath = "./data/data.pickle"
match_pickle_filepath = "./data/matches.pickle"


def all_mondays(year):  # Utility method to fetch all mondays in a given year
    return pd.date_range(start=str(year), end=str(year + 1), freq='W-MON').tolist()


def all_ranking_days():
    days = []
    firstRanking = pd.to_datetime(pd.Timestamp(year=2015, month=10, day=26))
    for i in range(2015, 2020):
        days += all_mondays(i)

    truedays = []
    for day in days:
        d = day.to_pydatetime()
        print(d)
        if d >= firstRanking:
            truedays.append(d)

    return truedays


def scrape_rankings():
    days = all_ranking_days()
    rows = []
    for i in range(0, len(days)):
        day = days[i]
        next_day = days[i + 1] - \
            datetime.timedelta(days=1) if i < len(days) - 1 else date.today()
        url = "https://www.hltv.org/ranking/teams/" + \
            str(day.year) + "/" + \
            months[day.month] + "/" + str(day.day)
        print("Sending request to: " + url)
        print("\n")
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        team_list = soup.findAll("div", class_="ranked-team standard-box")
        pattern = re.compile("\#(\d+)")
        res = []
        for team in team_list:
            rank = pattern.match(
                team.find("span", class_="position").text).groups(1)[0]
            name = team.find("span", class_="name").text
            players = team.findAll("div", class_="nick")
            playernames = [player.text for player in players]
            date_range = pd.date_range(start=day, end=next_day)
            for d in date_range:
                res.append([d, name, rank, playernames])

        print(res)
        rows += res
        print("\n")

    df = pd.DataFrame(data=rows, columns=["date", "name", "rank", "players"])
    return df


if not os.path.exists(ranking_pickle_filepath):
    rankings_df = scrape_rankings()
    rankings_df.to_pickle(ranking_pickle_filepath)
else:
    rankings_df = pd.read_pickle(ranking_pickle_filepath)

# ----------------------------------------------------- PLAYER DATA -----------------------------------------------------

players_link = "https://www.hltv.org/stats/players?startDate=all&matchType=Lan&rankingFilter=Top30"


def scrape_player_links():
    page = requests.get(players_link)
    soup = BeautifulSoup(page.content, "html.parser")
    players_table = soup.find(
        "table", class_="stats-table player-ratings-table")
    table_body = players_table.find("tbody")
    player_cells = table_body.findAll("tr")
    player_links = {}
    for player_cell in player_cells:
        player_col = player_cell.find("td", class_="playerCol")
        player_link_tag = player_col.find("a")
        player_link = player_link_tag["href"]
        player_name = player_link_tag.text
        player_links[player_name] = "https://www.hltv.org" + player_link

    return player_links


def scrape_player_data(links):
    data = []
    score_pattern = re.compile("\((\d+)\)")
    kd_pattern = re.compile("(\d+) - (\d+)")
    for player, link in links.items():
        matches_link = link.replace("/players", "/players/matches")
        print("processing " + matches_link + "\n---------------------")
        page = requests.get(matches_link)
        soup = BeautifulSoup(page.content, "html.parser")
        stats_table = soup.find("table")
        table_body = stats_table.find("tbody")
        match_rows = table_body.findAll("tr")
        for match_row in match_rows:
            cells = match_row.findAll("td")
            date = cells[0].find("div", class_="time").text.strip()
            team = cells[1].findAll("span")[0].text.strip()
            rounds_text = cells[1].findAll("span")[1].text.strip()
            team_rounds = score_pattern.match(rounds_text).group(1)
            opposing_team = cells[2].findAll("span")[0].text.strip()
            opposing_team_rounds = score_pattern.match(
                cells[2].findAll("span")[1].text.strip()).group(1)
            map_played = cells[3].text.strip()
            kills = kd_pattern.match(cells[4].text.strip()).group(1)
            deaths = kd_pattern.match(cells[4].text.strip()).group(2)
            differential = cells[5].text.strip()
            rating = cells[6].text.strip()
            data.append([player, date, team, team_rounds, opposing_team,
                         opposing_team_rounds, map_played, kills, deaths, differential, rating])

    columns = ["player", "date", "team", "team_rounds", "opposing_team",
               "opposing_team_rounds", "map", "kills", "deaths", "differential", "rating"]
    df = pd.DataFrame(data=data, columns=columns)

    return df


if not os.path.exists(match_pickle_filepath):
    player_links = scrape_player_links()
    player_data = scrape_player_data(player_links)
    player_data.to_pickle(match_pickle_filepath)
else:
    player_data = pd.read_pickle(match_pickle_filepath)


# ----------------------------------------------------- Tidying Data -----------------------------------------------------

tidied_rankings_path = "./data/tidied/rankings.pickle"
tidied_player_data_path = "./data/tidied/matches.pickle"

if not os.path.exists(tidied_rankings_path):
    rankings_df["date"] = pd.to_datetime(
        rankings_df["date"], format="%Y-%m-%d")
    rankings_df.to_pickle(tidied_rankings_path)
else:
    rankings_df = pd.read_pickle(tidied_rankings_path)


def get_opponent_rank(opponent, date):
    subset = rankings_df[rankings_df["date"] == date]
    if len(subset) == 0:
        return -1
    rows = subset[subset["name"] == opponent]
    if(len(rows) == 0):
        return -1
    row = rows.iloc[[0]]
    if len(row) == 0:
        return -1
    return row["rank"].iloc[0]


if not os.path.exists(tidied_player_data_path):
    # correct data formats

    player_data["date"] = pd.to_datetime(
        player_data["date"], format="%d/%m/%y")
    player_data["team_rounds"] = pd.to_numeric(player_data["team_rounds"])
    player_data["opposing_team_rounds"] = pd.to_numeric(
        player_data["opposing_team_rounds"])
    player_data["kills"] = pd.to_numeric(player_data["kills"])
    player_data["deaths"] = pd.to_numeric(player_data["deaths"])
    player_data["differential"] = pd.to_numeric(player_data["differential"])
    player_data["rating"] = player_data["rating"].replace("*", "")
    player_data["rating"] = player_data["rating"].str.strip()

    # adding necessary fields
    player_data["win"] = player_data["team_rounds"] > player_data["opposing_team_rounds"]
    player_data["kdr"] = player_data["kills"] / player_data["deaths"]

    player_data["opposing_team_rank"] = -1  # just to create the column
    for index, row in player_data.iterrows():
        print("Processing " + row["player"] + " vs. " +
              row["opposing_team"] + " on " + str(row["date"]))

        player_data.at[index, "opposing_team_rank"] = get_opponent_rank(
            row["opposing_team"], row["date"])

    # filtering matches to fit desired timeframe
    start = pd.to_datetime('2015-10-26')
    end = pd.to_datetime('2019-12-30')
    player_data = player_data.query(
        'date > @start and date < @end').reset_index(drop=True)

    player_data.to_pickle(tidied_player_data_path)

else:
    player_data = pd.read_pickle(tidied_player_data_path)


print(rankings_df.head())
print(len(rankings_df))
print(player_data.head())
print(len(player_data))
