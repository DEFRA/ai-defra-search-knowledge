-- Seed test data for knowledge_vectors table
-- This creates test knowledge base entries with the snapshot_id 'kg_34vf0wr3e06l'

DELETE FROM knowledge_vectors;

INSERT INTO knowledge_vectors (content, embedding, snapshot_id, source_id, metadata, created_at)
VALUES
  (
    'User-Centered Design (UCD) is an iterative design process that focuses on users and their needs throughout the design and development process.',
    array_fill(0.1, ARRAY[1024])::vector(1024),
    'kg_34vf0wr3e06l',
    'doc_001',
    '{"title": "What is UCD", "category": "design", "author": "Defra Digital"}'::jsonb,
    NOW()
  ),
  (
    'Good UCD practice involves conducting user research, creating personas, prototyping solutions, and testing with real users iteratively.',
    array_fill(0.2, ARRAY[1024])::vector(1024),
    'kg_34vf0wr3e06l',
    'doc_002',
    '{"title": "UCD Good Practice", "category": "design", "author": "Defra Digital"}'::jsonb,
    NOW()
  ),
  (
    'Defra uses UCD principles to ensure that digital services meet the needs of farmers, landowners, and environmental professionals.',
    array_fill(0.3, ARRAY[1024])::vector(1024),
    'kg_34vf0wr3e06l',
    'doc_003',
    '{"title": "Defra and UCD", "category": "government", "author": "Defra Digital"}'::jsonb,
    NOW()
  ),
  (
    'Accessibility is a core principle of UCD, ensuring that services are usable by people with diverse abilities and needs.',
    array_fill(0.4, ARRAY[1024])::vector(1024),
    'kg_34vf0wr3e06l',
    'doc_004',
    '{"title": "Accessibility in UCD", "category": "accessibility", "author": "Defra Digital"}'::jsonb,
    NOW()
  ),
  (
    'Service design at Defra follows government service standards and incorporates continuous user feedback and iteration.',
    array_fill(0.5, ARRAY[1024])::vector(1024),
    'kg_34vf0wr3e06l',
    'doc_005',
    '{"title": "Defra Service Standards", "category": "government", "author": "Defra Digital"}'::jsonb,
    NOW()
  );
