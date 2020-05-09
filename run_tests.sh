#!/bin/bash
export $(cat .env | sed -e /^$/d -e /^#/d | xargs)
pytest -s -vvvv --cov=hawk --cov-report term-missing
