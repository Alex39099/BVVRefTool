from abc import ABC, abstractmethod
import logging
from typing import Any

import pygsheets

logger = logging.getLogger(__name__)


def lib():
    gc_credentials = None
    gc = pygsheets.authorize(custom_credentials=gc_credentials)
    
    team_drive_id = ""
    gc.drive.enable_team_drive(team_drive_id)
    
    
    spreadsheet_id = ""
    spreadsheet = gc.open_by_key(spreadsheet_id)
    ss = gc.create(title="Some title", template="SOME TEMPLATE OBJ")
    
    template_wks = spreadsheet.worksheet_by_title('course_template')
    
    wks = spreadsheet.add_worksheet(
        title="Neuer Lehrgang...",
        src_worksheet=template_wks
    )
    
    
    
    


class BaseSheet(ABC):
    pass


class CourseSheet(BaseSheet):
    pass

class OverviewSheet(BaseSheet):
    pass

class MemberSheet(BaseSheet):
    pass




    