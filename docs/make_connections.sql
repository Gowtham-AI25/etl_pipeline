

ALTER TABLE [papers_citation_data] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_reference_ids] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_citation_data] ([paper_id])
GO

ALTER TABLE [papers_related_work_ids] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_citation_data] ([paper_id])
GO

ALTER TABLE [papers_paper_author] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_paper_author] ADD FOREIGN KEY ([author_id]) REFERENCES [papers_author_data] ([author_id])
GO

ALTER TABLE [papers_publication_data] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_indexed_in] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_opensource_access] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_topics_data] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_all_topics] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_keywords] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_concepts] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [papers_paper_availability] ADD FOREIGN KEY ([paper_id]) REFERENCES [papers_main] ([paper_id])
GO

ALTER TABLE [concepts_ancestor_concepts] ADD FOREIGN KEY ([concept_id]) REFERENCES [concepts_main] ([concept_id])
GO

ALTER TABLE [concepts_ancestor_concepts] ADD FOREIGN KEY ([ancestor_con_id]) REFERENCES [concepts_main] ([concept_id])
GO

ALTER TABLE [concepts_related_concepts] ADD FOREIGN KEY ([concept_id]) REFERENCES [concepts_main] ([concept_id])
GO

ALTER TABLE [concepts_related_concepts] ADD FOREIGN KEY ([related_concepts_id]) REFERENCES [concepts_main] ([concept_id])
GO

ALTER TABLE [concepts_counts_by_year] ADD FOREIGN KEY ([concept_id]) REFERENCES [concepts_main] ([concept_id])
GO

ALTER TABLE [papers_concepts] ADD FOREIGN KEY ([concept_id]) REFERENCES [concepts_main] ([concept_id])
GO

ALTER TABLE [funder_counts_by_years] ADD FOREIGN KEY ([funder_id]) REFERENCES [funders_main] ([funder_id])
GO

ALTER TABLE [funder_alt_titles] ADD FOREIGN KEY ([funder_id]) REFERENCES [funders_main] ([funder_id])
GO

ALTER TABLE [funder_extra_roles] ADD FOREIGN KEY ([funder_id]) REFERENCES [funders_main] ([funder_id])
GO

ALTER TABLE [funder_associated_concepts] ADD FOREIGN KEY ([funder_id]) REFERENCES [funders_main] ([funder_id])
GO

ALTER TABLE [sources_counts_by_year] ADD FOREIGN KEY ([source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [sources_associated_concepts] ADD FOREIGN KEY ([source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [sources_host_org_lineage_ids] ADD FOREIGN KEY ([source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [sources_associated_topics] ADD FOREIGN KEY ([source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [sources_alternate_titles] ADD FOREIGN KEY ([source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [sources_topic_share] ADD FOREIGN KEY ([source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [papers_publication_data] ADD FOREIGN KEY ([primary_source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [papers_paper_availability] ADD FOREIGN KEY ([location_source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [papers_opensource_access] ADD FOREIGN KEY ([best_oa_source_id]) REFERENCES [sources_main] ([source_id])
GO

ALTER TABLE [institution_alternatives_names] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_institution_roles] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_acronyms] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_lineage_ids] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_related_institutions] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_geo_id] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_geo_id] ADD FOREIGN KEY ([geonames_city_id]) REFERENCES [institution_geo_data] ([geonames_city_id])
GO

ALTER TABLE [institution_associated_concepts] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_counts_by_year] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_associated_topics] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [institution_topic_share] ADD FOREIGN KEY ([institution_id]) REFERENCES [institution_main] ([institution_id])
GO

ALTER TABLE [lookup_sibling_topics] ADD FOREIGN KEY ([topic_id]) REFERENCES [lookup_main] ([topic_id])
GO

ALTER TABLE [lookup_sibling_topics] ADD FOREIGN KEY ([sibling_id]) REFERENCES [lookup_main] ([topic_id])
GO

ALTER TABLE [lookup_topic_keywords] ADD FOREIGN KEY ([topic_id]) REFERENCES [lookup_main] ([topic_id])
GO

ALTER TABLE [lookup_parent_lookup] ADD FOREIGN KEY ([topic_id]) REFERENCES [lookup_main] ([topic_id])
GO

ALTER TABLE [lookup_parent_lookup] ADD FOREIGN KEY ([subfield]) REFERENCES [lookup_subfield_data] ([subfield_id])
GO

ALTER TABLE [lookup_parent_lookup] ADD FOREIGN KEY ([field]) REFERENCES [lookup_field_data] ([field_id])
GO

ALTER TABLE [lookup_parent_lookup] ADD FOREIGN KEY ([domain]) REFERENCES [lookup_domain_data] ([domain_id])
GO
