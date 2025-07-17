CREATE TABLE [papers_main] (
  [paper_id] nvarchar(40) PRIMARY KEY,
  [doi] nvarchar(255),
  [title] nvarchar(max),
  [abstract] nvarchar(max),
  [pub_date] date,
  [language] nvarchar(10),
  [publication_type] nvarchar(100),
  [num_locations] smallint,
  [linked_datasets] bit,
  [pubmed_id] nvarchar(50),
  [mag_id] nvarchar(50)
)
GO

CREATE TABLE [papers_citation_data] (
  [paper_id] nvarchar(40),
  [citation_percentile] decimal(5,2),
  [is_top_1_percent] bit,
  [citation_count] int,
  [field_weighted_citation_impact] decimal(5,2),
  [reference_count] int
)
GO

CREATE TABLE [papers_reference_ids] (
  [paper_id] nvarchar(40) ,
  [reference_ids] nvarchar(max)
)
GO

CREATE TABLE [papers_related_work_ids] (
  [paper_id] nvarchar(40) ,
  [related_work_ids] nvarchar(max)
)
GO

CREATE TABLE [papers_author_data] (
  [author_id] nvarchar(40) PRIMARY KEY,
  [author_name] nvarchar(255),
  [author_orcid] nvarchar(50),
  [author_countries] nvarchar(max),
  [author_institutions] nvarchar(max)
)
GO

CREATE TABLE [papers_paper_author] (
  [paper_id] nvarchar(40),
  [author_id] nvarchar(40),
  [is_corresponding_author] bit,
  [author_position] nvarchar(50)
)
GO

CREATE TABLE [papers_publication_data] (
  [paper_id] nvarchar(40),
  [primary_source_id] nvarchar(40),
  [is_oa_primary] bit,
  [primary_landing_page] nvarchar(500),
  [primary_pdf_url] nvarchar(500),
  [primary_license] nvarchar(100),
  [primary_is_published] bit,
  [volume] nvarchar(20),
  [issue] nvarchar(20),
  [apc_usd] money
)
GO

CREATE TABLE [papers_indexed_in] (
  [paper_id] nvarchar(40),
  [indexed_in] nvarchar(100)
)
GO

CREATE TABLE [papers_opensource_access] (
  [paper_id] nvarchar(40),
  [is_open_access] bit,
  [oa_status] nvarchar(100),
  [oa_url] nvarchar(500),
  [best_oa_source_id] nvarchar(40),
  [is_best_oa] bit,
  [best_oa_landing_page] nvarchar(500),
  [best_oa_pdf_url] nvarchar(500),
  [best_oa_license] nvarchar(100),
  [best_oa_is_published] bit
)
GO

CREATE TABLE [papers_topics_data] (
  [paper_id] nvarchar(40) ,
  [primary_topic_id] nvarchar(40),
  [primary_topic_score] decimal(6,4)
)
GO

CREATE TABLE [papers_all_topics] (
  [paper_id] nvarchar(40),
  [topic_id] nvarchar(40),
  [topic_score] decimal(6,4)
)
GO

CREATE TABLE [papers_keywords] (
  [paper_id] nvarchar(40),
  [keyword] nvarchar(255),
  [keyword_score] decimal(6,4)
)
GO

CREATE TABLE [papers_concepts] (
  [paper_id] nvarchar(40),
  [concept_id] nvarchar(40),
  [concept_score] decimal(6,4)
)
GO

CREATE TABLE [papers_paper_availability] (
  [paper_id] nvarchar(40),
  [location_source_id] nvarchar(40),
  [location_is_oa] bit,
  [location_landing_page] nvarchar(500),
  [location_pdf_url] nvarchar(500),
  [location_license] nvarchar(100),
  [location_is_published] bit
)
GO

CREATE TABLE [concepts_main] (
  [concept_id] nvarchar(40) PRIMARY KEY,
  [display_name] nvarchar(255),
  [hierarchy_level] smallint,
  [description] nvarchar(max),
  [works_count] int,
  [cited_by_count] int,
  [2yr_mean_citedness] decimal(7,4),
  [h_index] int,
  [i10_index] int,
  [oa_percent] decimal(5,2)
)
GO

CREATE TABLE [concepts_ancestor_concepts] (
  [concept_id] nvarchar(40),
  [ancestor_con_id] nvarchar(40)
)
GO

CREATE TABLE [concepts_related_concepts] (
  [concept_id] nvarchar(40),
  [related_concepts_id] nvarchar(40),
  [score] decimal(7,4)
)
GO

CREATE TABLE [concepts_counts_by_year] (
  [concept_id] nvarchar(40),
  [year] smallint,
  [works_count] int,
  [oa_works_count] int,
  [cited_by_count] int
)
GO

CREATE TABLE [funders_main] (
  [funder_id] nvarchar(40) PRIMARY KEY,
  [display_name] nvarchar(255),
  [country_code] nvarchar(10),
  [description] nvarchar(max),
  [homepage_url] nvarchar(500),
  [grants_count] int,
  [works_count] int,
  [citation_count] int,
  [crossref] nvarchar(255),
  [doi] nvarchar(100),
  [ror] nvarchar(100),
  [h_index] int,
  [i10_index] int,
  [oa_percent] decimal(5,2),
  [2yr_works_count] int,
  [2yr_cited_by_count] int,
  [role] nvarchar(100)
)
GO

CREATE TABLE [funder_counts_by_years] (
  [funder_id] nvarchar(40),
  [year] smallint,
  [works_count] int,
  [oa_works_count] int,
  [cited_by_count] int
)
GO

CREATE TABLE [funder_alt_titles] (
  [funder_id] nvarchar(40),
  [alt_titles] nvarchar(max)
)
GO

CREATE TABLE [funder_extra_roles] (
  [funder_id] nvarchar(40),
  [role] nvarchar(100),
  [id] nvarchar(40),
  [works_count] int
)
GO

CREATE TABLE [funder_associated_concepts] (
  [funder_id] nvarchar(40),
  [id] nvarchar(40),
  [score] decimal(7,4)
)
GO

CREATE TABLE [sources_main] (
  [source_id] nvarchar(40) PRIMARY KEY,
  [name] nvarchar(255),
  [publisher] nvarchar(255),
  [host_org_id] nvarchar(40),
  [is_oa] bit,
  [is_in_doaj] bit,
  [is_core_journal] bit,
  [is_indexed_in_scopus] bit,
  [source_type] nvarchar(100),
  [works_count] int,
  [cited_by_count] int,
  [homepage_url] nvarchar(500),
  [country_code] nvarchar(10),
  [apc_usd] money,
  [2yr_mean_citedness] decimal(6,4),
  [h_index] int,
  [i10_index] int,
  [oa_percent] decimal(5,2)
)
GO

CREATE TABLE [sources_counts_by_year] (
  [source_id] nvarchar(40),
  [year] smallint,
  [works_count] int,
  [oa_works_count] int,
  [cited_by_count] int
)
GO

CREATE TABLE [sources_associated_concepts] (
  [source_id] nvarchar(40),
  [concept_id] nvarchar(40),
  [level] smallint,
  [score] decimal(7,4)
)
GO

CREATE TABLE [sources_host_org_lineage_ids] (
  [source_id] nvarchar(40),
  [host_org_lineage_ids] nvarchar(max)
)
GO

CREATE TABLE [sources_associated_topics] (
  [source_id] nvarchar(40),
  [topic_id] nvarchar(40),
  [count] int
)
GO

CREATE TABLE [sources_alternate_titles] (
  [index] int ,
  [source_id] nvarchar(40),
  [alternate_titles] nvarchar(max)
)
GO

CREATE TABLE [sources_topic_share] (
  [source_id] nvarchar(40),
  [topic_id] nvarchar(40),
  [value] decimal(6,4)
)
GO

CREATE TABLE [institution_main] (
  [institution_id] nvarchar(40) PRIMARY KEY,
  [ror] nvarchar(100),
  [display_name] nvarchar(255),
  [country_code] nvarchar(10),
  [institution_type] nvarchar(100),
  [homepage_url] nvarchar(1000),
  [works_count] int,
  [cited_by_count] int,
  [2yr_mean_citedness] decimal(7,4),
  [h_index] int,
  [i10_index] int,
  [oa_percent] decimal(6,4)
)
GO

CREATE TABLE [institution_alternatives_names] (
  [institution_id] nvarchar(40),
  [alternatives_names] nvarchar(max)
)
GO

CREATE TABLE [institution_institution_roles] (
  [institution_id] nvarchar(40),
  [role] nvarchar(100),
  [id] nvarchar(40),
  [works_count] int
)
GO

CREATE TABLE [institution_acronyms] (
  [institution_id] nvarchar(40),
  [acronyms] nvarchar(max)
)
GO

CREATE TABLE [institution_lineage_ids] (
  [institution_id] nvarchar(40),
  [lineage_ids] nvarchar(max)
)
GO

CREATE TABLE [institution_related_institutions] (
  [institution_id] nvarchar(40),
  [id] nvarchar(40),
  [relationship] nvarchar(100)
)
GO

CREATE TABLE [institution_geo_data] (
  [geonames_city_id] nvarchar(40) PRIMARY KEY,
  [city] nvarchar(255),
  [country_code] nvarchar(10),
  [country] nvarchar(100),
  [latitude] decimal(9,6),
  [longitude] decimal(9,6)
)
GO

CREATE TABLE [institution_geo_id] (
  [institution_id] nvarchar(40),
  [geonames_city_id] nvarchar(40)
)
GO

CREATE TABLE [institution_associated_concepts] (
  [institution_id] nvarchar(40),
  [concept_id] nvarchar(40),
  [score] decimal(7,4)
)
GO

CREATE TABLE [institution_counts_by_year] (
  [institution_id] nvarchar(40),
  [year] smallint,
  [works_count] int,
  [oa_works_count] int,
  [cited_by_count] int
)
GO

CREATE TABLE [institution_associated_topics] (
  [institution_id] nvarchar(40),
  [ass_topic_id] nvarchar(40),
  [count] int
)
GO

CREATE TABLE [institution_topic_share] (
  [institution_id] nvarchar(40),
  [topic_id] nvarchar(40),
  [value] decimal(7,4)
)
GO

CREATE TABLE [lookup_main] (
  [topic_id] nvarchar(40) PRIMARY KEY,
  [display_name] nvarchar(255),
  [description] nvarchar(max),
  [works_count] int,
  [cited_by_count] int
)
GO

CREATE TABLE [lookup_subfield_data] (
  [subfield_id] nvarchar(40) PRIMARY KEY,
  [display_name] nvarchar(255)
)
GO

CREATE TABLE [lookup_field_data] (
  [field_id] nvarchar(40) PRIMARY KEY,
  [display_name] nvarchar(255)
)
GO

CREATE TABLE [lookup_domain_data] (
  [domain_id] nvarchar(40) PRIMARY KEY,
  [display_name] nvarchar(255)
)
GO

CREATE TABLE [lookup_sibling_topics] (
  [topic_id] nvarchar(40),
  [sibling_id] nvarchar(40)
)
GO

CREATE TABLE [lookup_topic_keywords] (
  [topic_id] nvarchar(40),
  [keywords] nvarchar(max)
)
GO

CREATE TABLE [lookup_parent_lookup] (
  [topic_id] nvarchar(40) PRIMARY KEY,
  [subfield] nvarchar(40),
  [field] nvarchar(40),
  [domain] nvarchar(40)
)
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Define composite PK with paper_id + author_id if required',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'papers_paper_author';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: concept_id + ancestor_con_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'concepts_ancestor_concepts';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: concept_id + related_concepts_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'concepts_related_concepts';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: concept_id + year',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'concepts_counts_by_year';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: funder_id + year',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'funder_counts_by_years';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: funder_id + role + id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'funder_extra_roles';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: funder_id + id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'funder_associated_concepts';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: source_id + year',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'sources_counts_by_year';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: source_id + concept_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'sources_associated_concepts';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: source_id + topic_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'sources_associated_topics';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: source_id + topic_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'sources_topic_share';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + role + id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_institution_roles';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_related_institutions';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + geonames_city_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_geo_id';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + concept_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_associated_concepts';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + year',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_counts_by_year';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + ass_topic_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_associated_topics';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: institution_id + topic_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'institution_topic_share';
GO

EXEC sp_addextendedproperty
@name = N'Table_Description',
@value = 'Composite PK: topic_id + sibling_id',
@level0type = N'Schema', @level0name = 'dbo',
@level1type = N'Table',  @level1name = 'lookup_sibling_topics';
GO

