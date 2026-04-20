from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Dict, Literal,Any
from pydantic import ValidationError
import json


class BulkTemplateRequest(BaseModel):
    product_ids: List[str]


class ProductImportRow(BaseModel):
    sku: Optional[str] = None
    parent_sku: Optional[str] = Field(None, alias="parent sku / variation group code")

    title: str = Field(..., alias="title")
    brand_name: str = Field(..., alias="brand name")
    category_code: str = Field(..., alias="category code")
    description: str = Field(..., alias="long description")
    tags: Optional[List[str]] = Field(None, alias="product tag")
    rrp: Optional[float] = Field(None, alias="rrp")
    selling_price: Optional[float] = Field(None, alias="selling price")
    stock_quantity: Optional[int] = Field(None, alias="stock quantity")

    weight: Optional[float] = Field(None, alias="package weight (kg)")
    length: Optional[float] = Field(None, alias="package length(cms)")
    width: Optional[float] = Field(None, alias="package width(cms)")
    height: Optional[float] = Field(None, alias="package height(cms)")

    # SEO fields
    seo_keywords: Optional[str] = Field(None, alias="SEO Keywords")
    page_title: Optional[str] = Field(None, alias="Page Title")
    meta_description: Optional[str] = Field(None, alias="Meta Description")
    url_handle: Optional[str] = Field(None, alias="URL Handles")
    canonical_url: Optional[str] = Field(None, alias="Canonical URL")

    # ---------------- TAG PARSER ----------------
    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags(cls, v):
        if v is None or v == "":
            return None

        # already a list → fine
        if isinstance(v, list):
            return v

        # CSV gives string → try JSON
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass

            # fallback: comma-separated
            return [tag.strip() for tag in v.split(",") if tag.strip()]

        raise ValueError("product tag must be a list or comma-separated string")

    # ------------------ BASIC FIELD VALIDATORS (V2 STYLE) ------------------
    @model_validator(mode="after")
    def validate_price_relation(self):
        if self.rrp is not None and self.selling_price is not None:
            if self.selling_price > self.rrp:
                raise ValueError(
                    f"Selling price ({self.selling_price}) can not be greater than RRP price ({self.rrp})"
                )
        return self

    @field_validator("brand_name")
    def brand_required(cls, v):
        if not v or not str(v).strip():
            raise ValueError("Brand name is required")
        return v

    @field_validator("category_code")
    def category_required(cls, v):
        if not v or not str(v).strip():
            raise ValueError("Category code is required")
        return v

    @field_validator("title")
    def title_required(cls, v):
        if not v or not v.strip():
            raise ValueError("Title is required")
        return v

    @field_validator("sku")
    def sku_required(cls, v):
        if not v or not v.strip():
            raise ValueError("SKU is required")
        return v

    # ------------------ NUMERIC VALIDATION (Pydantic v2 format) ------------------

    @field_validator("weight", "length", "width", "height", mode="before")
    def validate_dimensions(cls, v, info):
        if v is None or v == "":
            return None
        try:
            num = float(v)
        except Exception:
            raise ValueError(f"{info.field_name} must be a valid number")

        if num <= 0:
            raise ValueError(f"{info.field_name} must be greater than 0")

        return num


def format_pydantic_error(e: ValidationError):
    msgs = []
    for err in e.errors():
        field = " → ".join(str(x) for x in err["loc"])
        msg = err["msg"]
        msgs.append(f"{field}: {msg}")
    return msgs


class ProductExportRequest(BaseModel):
    product_ids: Optional[List[str]] = None

    filters: Optional[Dict[str, Any]] = None
    sort: Optional[str] = None

    download_flag: Literal["csv", "excel"] = "excel"
    columns: Optional[List[str]] = None
    background: bool = False
