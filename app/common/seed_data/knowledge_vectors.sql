-- Seed test data for knowledge_vectors table
-- This creates test knowledge base entries with the snapshot_id 'kg_34vf0wr3e06l'

DELETE FROM knowledge_vectors;

-- Seed test data for knowledge_vectors table
-- knowledge_group_id 674f1f77bcf86cd799439011 matches MongoDB knowledgeGroups._id
-- source_id matches documents._id for RAG enrichment

INSERT INTO knowledge_vectors (content, embedding, source_id, metadata, created_at)
VALUES
    (
        'User-Centered Design (UCD) is an iterative design process that focuses on users and their needs throughout the design and development process.',
        array_fill(0.1, ARRAY[1024])::vector(1024),
        '674f1f77bcf86cd799439012',
        '{"knowledge_group_id": "674f1f77bcf86cd799439011", "source": "What is UCD"}'::jsonb,
        NOW()
    ),
    (
        'Good UCD practice involves conducting user research, creating personas, prototyping solutions, and testing with real users iteratively.',
        array_fill(0.2, ARRAY[1024])::vector(1024),
        '674f1f77bcf86cd799439013',
        '{"knowledge_group_id": "674f1f77bcf86cd799439011", "source": "UCD Good Practice"}'::jsonb,
        NOW()
    ),
    (
        'Defra uses UCD principles to ensure that digital services meet the needs of farmers, landowners, and environmental professionals.',
        array_fill(0.3, ARRAY[1024])::vector(1024),
        '674f1f77bcf86cd799439014',
        '{"knowledge_group_id": "674f1f77bcf86cd799439011", "source": "Defra and UCD"}'::jsonb,
        NOW()
    ),
    (
        'Accessibility is a core principle of UCD, ensuring that services are usable by people with diverse abilities and needs.',
        array_fill(0.4, ARRAY[1024])::vector(1024),
        '674f1f77bcf86cd799439015',
        '{"knowledge_group_id": "674f1f77bcf86cd799439011", "source": "Accessibility in UCD"}'::jsonb,
        NOW()
    ),
    (
        'Service design at Defra follows government service standards and incorporates continuous user feedback and iteration.',
        array_fill(0.5, ARRAY[1024])::vector(1024),
        '674f1f77bcf86cd799439016',
        '{"knowledge_group_id": "674f1f77bcf86cd799439011", "source": "Defra Service Standards"}'::jsonb,
        NOW()
    );

-- Verify the data was inserted
SELECT COUNT(*) as total_vectors FROM knowledge_vectors WHERE metadata->>'knowledge_group_id' = '674f1f77bcf86cd799439011';
