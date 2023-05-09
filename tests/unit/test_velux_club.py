import pytest
import pandas as pd

from viadot.sources import VeluxClub, Historical_Too_Old, Source_NOK, Dates_NOK

def test_veluxclub_HISTORICAL_TOO_OLD_failure():
        with pytest.raises(Historical_Too_Old) as exc_info:   
            velux_club = VeluxClub(config_key='velux_club')
            df = velux_club.get_response(source="product", from_date="2021-01-01", to_date="2023-01-31")

def test_veluxclub_WRONG_SOURCE_failure():
        with pytest.raises(Source_NOK) as exc_info:   
            velux_club = VeluxClub(config_key='velux_club')
            df = velux_club.get_response(source="product1", from_date="2023-01-01", to_date="2023-01-31")

def test_veluxclub_DATES_NOK_failure():
        with pytest.raises(Dates_NOK) as exc_info:   
            velux_club = VeluxClub(config_key='velux_club')
            df = velux_club.get_response(source="product", from_date="2023-03-01", to_date="2023-01-31")


def test_veluxclub_connexion_product():
        velux_club = VeluxClub(config_key='velux_club')
        df = velux_club.get_response(source="product", from_date="2023-01-01", to_date="2023-01-31")

        cols_expected = ['submissionProductID', 'submissionID', 'regionID', 'productCode', 'productQuantity', 'submissionProductDate', 'brand', 'unit']
        length_expected = 2618
        assert list(df.columns) == cols_expected and len(df.index)==length_expected

def test_veluxclub_connexion_survey():
        velux_club = VeluxClub(config_key='velux_club')
        df = velux_club.get_response(source="survey", from_date="2023-01-01", to_date="2023-01-31")

        cols_expected = ['id', 'type', 'text']
        length_expected = 18
        assert list(df.columns) == cols_expected and len(df.index)==length_expected


def test_veluxclub_connexion_company():
        velux_club = VeluxClub(config_key='velux_club')
        df = velux_club.get_response(source="company", from_date="2023-01-01", to_date="2023-01-31")

        cols_expected = ['customerID', 'companyName', 'address1', 'town', 'postcode', 'companyNumber', 'country', 'serviceTechnician', 'areaManager', 'ASEID', 'customerType', 'membershipDate', 'status', 'firstName', 'lastName', 'email', 'msisdn', 'languageID', 'numberOfClosedProjects', 'numberOfOpenProjects', 'numberOfScans', 'totalPoints', 'pointsSpent', 'currentPoints', 'pendingPoints', 'expiredPoints', 'expiringPoints', 'rewardsOrdered', 'optinsms', 'optinemail', 'optinMarketing', 'lastlogin', 'totalLogins', 'q1Core', 'Column 1', 'Column 2', 'q2Core', 'q3Core', 'q4Core', 'q5Core', 'q6Core', 'q7Core', 'q8Core', 'Column 4', 'Column 3', 'Column 5', 'Column 6', 'Column 7', 'Column 8', 'Column 9', 'Column 10', 'Column 11', 'Column 12', 'Column 13']
        length_expected = 112
        assert list(df.columns) == cols_expected and len(df.index)==length_expected


def test_veluxclub_connexion_jobs():
        velux_club = VeluxClub(config_key='velux_club')
        df = velux_club.get_response(source="jobs", from_date="2023-01-01", to_date="2023-01-31")

        cols_expected = ['submissionID', 'submissionAddress', 'regionID', 'submissionPostcode', 'submissionDate', 'customerID', 'status', 'submissionQuantity', 'points', 'q1Core', 'q2Core', 'q2CoreOther', 'q3Core', 'q4Core', 'q5Core', 'q5CoreOther', 'q6Core', 'q6CoreOther', 'q7Core', 'q7CoreOther', 'q8Core', 'q8CoreOther', 'q9Core']
        length_expected = 743
        assert list(df.columns) == cols_expected and len(df.index)==length_expected