BEGIN;

-- Synthetic demonstration layout for the unchanged Pilot Site dashboard.
-- Existing floor records, assignments, or coordinates always win.
INSERT INTO floor(floor_id,site_id,name,width_m,height_m,rooms)
SELECT 'pilot-demo-floor',s.site_id,'Pilot Floor',60,40,'[]'::jsonb
FROM site s
WHERE s.site_id='site-01' AND s.name='Pilot Site'
  AND NOT EXISTS (SELECT 1 FROM floor f WHERE f.site_id=s.site_id)
ON CONFLICT DO NOTHING;

UPDATE ap a SET floor_id='pilot-demo-floor',x_m=v.x_m,y_m=v.y_m
FROM (VALUES
  ('ap-01',12.0,10.0),
  ('ap-02',45.0,10.0),
  ('ap-03',12.0,30.0),
  ('ap-04',45.0,30.0)
) AS v(ap_id,x_m,y_m)
WHERE a.ap_id=v.ap_id AND a.site_id='site-01'
  AND a.floor_id IS NULL AND a.x_m IS NULL AND a.y_m IS NULL
  AND EXISTS (SELECT 1 FROM floor f WHERE f.floor_id='pilot-demo-floor' AND f.site_id=a.site_id);

COMMIT;
