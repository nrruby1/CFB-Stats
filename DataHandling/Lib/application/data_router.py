from fastapi import APIRouter, Depends, HTTPException

from db.db_connection import *
from db.model.team import *
from db.model.team_repository import TeamRepository
from application.dependencies import get_db_connection

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/team", operation_id="get_team", response_model=Team)
async def get_team(
    school: str = None,
    team_id: int = None,
    year: int = 2025,
    db_client: DbConnection = Depends(get_db_connection),
) -> Team:
    if school is None and team_id is None:
        raise HTTPException(
            status_code=400, detail="'school' or 'team_id' parameter is required"
        )

    repo: TeamRepository = db_client.get_cfb_repository(Databases.production, Team)

    if team_id is not None:
        team = repo.find_team(year=year, team_id=team_id)
    else:
        team = repo.find_team(year=year, school=school)

    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")
    return team
