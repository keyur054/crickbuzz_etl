# This script fetch the apis data from cricbuzz apis,
# prepares the json fro the scores and uploads it to the cloud storage data lake.

import pandas as pd
import requests
import logging

class fetchCricketData:

  # url for the api and match id which will be used to fetch the specific
  # match's score.

  url = "https://unofficial-cricbuzz.p.rapidapi.com/matches/get-scorecard"
  querystring = {"matchId":"50921"}

  headers = {
      "X-RapidAPI-Key": "88f6f91fd0msh4e8e0f86448e525p181c08jsnc3ff883e206e",
      "X-RapidAPI-Host": "unofficial-cricbuzz.p.rapidapi.com"
      }

  individual_team_score_list = []
  individual_batsman_score_list = []
  
  # This global function will appned 0 if any json valus is missing.
  @staticmethod
  def appendNotExistedValue(listToAppend , key, fromListToCheck):
    if key in fromListToCheck:
      listToAppend[key] = fromListToCheck[key]
    else:
      listToAppend[key] = 0
    return listToAppend

  # This function will convert json to CSV and store to cloud storage.
  # @staticmethod
  # def convertAndSaveCSV(arrayList, name):
  #   df = pd.DataFrame(arrayList)
  #   #df.to_csv(name, index=False)
  #   df.to_csv(name, index = False)
  #   !gsutil cp name 'gs://cricbuzz_score_csv/'
  #   logging.info(name)

  
  # Request and prepare the csv for data lake
  @classmethod
  def requestandConvertApiResponse(cls):

    response = requests.request("GET", cls.url, headers=cls.headers, params=cls.querystring)

    score = response.json()

    #Iterating over the each team's inning's stats
    for scorecard_team in score['scorecard']:

      #Create array for the individual team
      individual_score = { 'runs' : scorecard_team['score'],
                          'wickets' :scorecard_team['wickets'],
                          'overs' :scorecard_team['overs'],
                          'runRate' :scorecard_team['runRate'],
                          'team' :scorecard_team['batTeamSName'],
                          'teamL' :scorecard_team['batTeamName'],
                          'inningsOrder' :scorecard_team['inningsId']
                          }

      cls.individual_team_score_list.append(individual_score)

      #Iterating in each batsman scorecard
      for batsaman_performance in scorecard_team['batsman']:
        batsman_score_list = { 
            'name' :batsaman_performance['name'],
            'strikeRate' :batsaman_performance['strkRate'],
            'team' : scorecard_team['batTeamSName']
            }

        batsman_score_list = cls.appendNotExistedValue(batsman_score_list, 'runs', batsaman_performance)
        batsman_score_list = cls.appendNotExistedValue(batsman_score_list, 'balls', batsaman_performance)
        batsman_score_list = cls.appendNotExistedValue(batsman_score_list, 'fours', batsaman_performance)
        batsman_score_list = cls.appendNotExistedValue(batsman_score_list, 'sixes', batsaman_performance)
        batsman_score_list = cls.appendNotExistedValue(batsman_score_list, 'outDec', batsaman_performance)

        cls.individual_batsman_score_list.append(batsman_score_list)

    df_team_score = pd.DataFrame(cls.individual_team_score_list)
    df_team_score.to_csv('team_score.csv', index = False)
    !gsutil cp team_score.csv 'gs://cricbuzz_score_csv/'

    df_batsman_score = pd.DataFrame(cls.individual_batsman_score_list)
    df_batsman_score.to_csv('batsman_scorecard.csv', index = False)
    !gsutil cp batsman_scorecard.csv 'gs://cricbuzz_score_csv/'
    # cls.convertAndSaveCSV(cls.individual_team_score_list,'team_score.csv')
    # cls.convertAndSaveCSV(cls.individual_batsman_score_list,'batsman_scorecard.csv')