from pydantic import BaseModel, Field
from datetime import datetime, date, time
from typing import Optional
from edgar import Filing as EdgarFiling

class SecFilingSchema(BaseModel):
    """
    Pydantic model for an SEC filing to be published to the aggregator.
    """

    accession_number: str
    cik: str
    company_name: str
    form_type: str
    filing_url: str
    
    filed_at: Optional[datetime]
    period_of_report: Optional[datetime] = None

    file_number: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # The new field you requested
    backend_source: str = "newsquawk-sec-filings"

    class Config:
        orm_mode = True  # This is not strictly needed if using .from_filing

    @classmethod
    def from_filing(cls, filing: EdgarFiling):
        """
        Helper method to create this schema from the 'edgar.Filing' object.
        """
        filed_at_dt: Optional[datetime] = None
        filing_date = filing.filing_date

        if isinstance(filing_date, datetime):
            filed_at_dt = filing_date
        elif isinstance(filing_date, date):
            filed_at_dt = datetime.combine(filing_date, time.min)
        elif isinstance(filing_date, str):
            try:
                # Use fromisoformat for 'YYYY-MM-DD' strings
                filed_at_dt = datetime.fromisoformat(filing_date)
            except (ValueError, TypeError):
                filed_at_dt = None # Handle bad format or None

        period_of_report_dt: Optional[datetime] = None
        report_period = filing.period_of_report
        
        if isinstance(report_period, datetime):
            period_of_report_dt = report_period
        elif isinstance(report_period, date):
            period_of_report_dt = datetime.combine(report_period, time.min)
        elif isinstance(report_period, str):
            try:
                # Use fromisoformat for 'YYYY-MM-DD' strings
                period_of_report_dt = datetime.fromisoformat(report_period)
            except (ValueError, TypeError):
                period_of_report_dt = None # Handle bad format or None

        # 3. Get file_number, handling potential AttributeError
        file_num: Optional[str] = None
        try:
            file_num = filing.file_number
        except AttributeError:
            file_num = None  # Explicitly set to None if attribute doesn't exist

        return cls(
            accession_number=filing.accession_number,
            cik=str(filing.cik),
            company_name=filing.company,
            form_type=filing.form,
            filing_url=filing.filing_url,
            
            # Pass the converted datetime objects
            filed_at=filed_at_dt,
            period_of_report=period_of_report_dt,
            
            # Pass the new file_number
            file_number=file_num,
            
            # created_at and updated_at are set by default_factory
        )