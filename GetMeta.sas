/************************************************
*************************************************
Started of from the code shared by Kong Kai Yun.
Reference: 
1. https://support.sas.com/resources/papers/proceedings09/097-2009.pdf
2. https://docs.datacontroller.io/dcu-lineage/
*************************************************
*************************************************/

options mprint mlogic symbolgen;

/************************************************
*************************************************
START: Please plot in the parameters accordingly
*************************************************/
options Metaport=8561;
options Metauser="sasdemo";
options Metapass="Orion123";
options METASERVER="sas-aap.demo.sas.com";
options Metarepository="Foundation";
options metaprotocol=bridge;

libname stordir "C:\THC\";
/************************************************
END: Please plot in the parameters accordingly
*************************************************
*************************************************/

%macro getMetaObj(objType, store, id=., mode=CREATE);
	%let quotedID=%sysfunc(quote(&id.,%str(%')));
	
	%if &mode.=CREATE %then %do;
		data stordir.&store.;
			length name $60 id $32 type $50;
			delete;
		run;
	%end;
	
	data TMP&store. (keep=name id type);
		length 
			uri $256 name $60 id $32 type $50;
		uri="";n=1;TableName="";
		type="&objType.";
		do while(metadata_getnobj("omsobj:&objType.?@Id?&quotedID.", n, uri) >= 0);
			rc=metadata_getattr(uri, 'Name', name);
			rc=metadata_getattr(uri, 'Id', id);
			n=n+1;
			output;
		end;
	run;
		
	proc append base=stordir.&store. data=TMP&store.;
	run;

	proc datasets library=WORK;
		delete TMP&store.;
	run;
%mend getMetaObj;

%macro getMetaObjs(objStore, store, objType);
	proc sql;
		select count(1) into :objCnt from stordir.&objStore.;
	quit;

	proc sql;
		select id into :id1-:id%trim(&objCnt.) from stordir.&objStore;
	quit;

	%let mode=CREATE;
	%do cnt=1 %to &objCnt.;
		%getMetaObj(&objType., &store., id=&&id&cnt.., mode=&mode.);
		%let mode=APPEND;
	%end;
%mend getMetaObjs;

%macro getMetaLink(parent_id, store, type, linkType1, linkType2);
	%let quotedID=%sysfunc(quote(&parent_id.,%str(%')));
	%let quotedLinkType1=%sysfunc(quote(&linkType1.,%str(%')));
	%let quotedLinkType2=%sysfunc(quote(&linkType2.,%str(%')));
	
	data TMP&store. (keep=parent_id id name seq watch
		%if &linkType1. = Transformations %then %do;
			library_name
			library_id
		%end;
		%else %do;
			role
		%end;
		);
		length parent_id $32 id $32 name $256 count 8 seq 8 watch $256;
		length this_uri $256 next_uri $256 link_uri $256;
		%if &linkType1. = Transformations %then %do;
			length library_name $256;
			length library_id $256;
		%end;
		%else %do;
			length role $256;
		%end;
		parent_id=&quotedID.;
		rc=metadata_getnobj("omsobj:&type.?@Id?&quotedID.", 1, this_uri);
		/* get association, supports a list in case we need deeper traversals */
		%let uriCnt=%sysfunc(countw(&linkType1., %str( )));
		%do u=1 %to &uriCnt.;
			%let uriType=%scan(&linkType1., &u., %str( ));
			%let quotedUriType=%sysfunc(quote(&uriType., %str(%')));
			rc=metadata_getnasn(this_uri, &quotedUriType., 1, link_uri);
			this_uri=link_uri;
		%end;
		rc=metadata_getnasn(this_uri, &quotedLinkType1., 1, link_uri);
		/* find out how many associated */
		count=metadata_getnasn(link_uri, &quotedLinkType2., 1, next_uri);
		
		do s=1 to count;
			/* get details of each step */
			seq=s;
			rc=metadata_getnasn(link_uri, &quotedLinkType2., s, next_uri);
			rc=metadata_getattr(next_uri, 'Id', id);
			rc=metadata_getattr(next_uri, 'Name', name);
			
			%if &linkType1. = Transformations %then %do;
				uri = next_uri;
				rc=metadata_getnasn(uri, 'TablePackage', 1, next_uri);
				watch=next_uri;
				if rc gt 0 then do;
					rc=metadata_getattr(next_uri, "Name", library_name);
					rc=metadata_getattr(next_uri, "Id", library_id);
				end;
			%end;
			%else %do;
				rc=metadata_getattr(next_uri, 'TransformRole', role);
			%end;
			output;
		end;
	run;
	
	proc append base=stordir.&store. data=TMP&store.;
	run;

	proc datasets library=WORK;
		delete TMP&store.;
	run;
%mend getMetaLink;

%macro getMetaLinks(objStore, store, linkType1, linkType2);
	data stordir.&store.;
		length parent_id $32 id $32 name $256 seq 8 watch $256;
		%if &linkType1. = Transformations %then %do;
			length library_name $256;
			length library_id $256;
		%end;
		%else %do;
			length role $256;
		%end;
		delete;
	run;
	
	proc sql;
		select count(1) into :objCnt from stordir.&objStore.;
	quit;
	
	proc sql;
		select
			id, type
		into
			:id1-:id%trim(&objCnt.),
			:type1-:type%trim(&objCnt.)
		from stordir.&objStore.;
	quit;
	
	%do cnt=1 %to &objCnt.;
		%getMetaLink(parent_id=&&id&cnt.., store=&store., type=&&type&cnt.., linkType1=&linkType1., linkType2=&linkType2.);
	%end;
%mend getMetaLinks;

%macro getTableDet(store, objType, key, id, structure, featureMap, endPoint);
	%let quotedID=%sysfunc(quote(&id., %str(%')));
	%let quotedFeatureMap=%sysfunc(quote(&featureMap.,%str(%')));
	%let quotedEndPoint=%sysfunc(quote(&endPoint.,%str(%')));

	data TMP&store.;
		%&structure.
		nobj=1;
		n=1;
		type="&objType.";
		nobj=metadata_getnobj("omsobj:&objType.?@&key.?&quotedID.", n, uri);
		rc=metadata_getattr(uri, 'Name', inp_tablenm);
		id=&quotedID.;
		col_cnt=0;
		rc2=-1;
		xxx='';
		yyy='';
		map_lib_nm='';
		map_tbl_nm='';
		map_col_nm='';
		map_col_id='';
		map_tbl_id='';
		map_lib_id='';
		map_trans_role='';
		map_rules='';
		

		if nobj>0 then do;
			do until (rc2 lt 0);
				watch='';
				tbl_mode='';
				col_cnt=col_cnt+1;
				rc2=metadata_getnasn(uri, 'Columns', col_cnt, next_uri2);
/*				OMSOBJ:Column\A5ED3SKF.BO0003LM*/
/*				watch=next_uri2;*/
				if rc2>0 then do;
					rc2=metadata_getattr(next_uri2, 'Name', inp_col_nm);
					rc2=metadata_getattr(next_uri2, 'Id', inp_col_id);
					uri3=next_uri2;
					rc3=metadata_getnasn(uri3, &quotedFeatureMap., 1, next_uri3);
/*					OMSOBJ:FeatureMap\A5ED3SKF.C500002X*/
/*					watch=next_uri3;*/
					if rc3 gt 0 then do;
						next_uri6='';
						rc3=metadata_getattr(next_uri3, 'TransformRole', map_trans_role);
						%if &featureMap. = SourceFeatureMaps %then %do;
							%let featureDest = 'FeatureTargets';
/*							%let featureDest = 'FeatureSources';*/
							tbl_mode='Target';
						%end;
						%else %if &featureMap. = TargetFeatureMaps %then %do;
							%let featureDest = 'FeatureSources';
/*							%let featureDest = 'FeatureTargets';*/
							tbl_mode='Source';
						%end;
						rc4=metadata_getnasn(next_uri3, &featureDest., 1, next_uri4);
/*						OMSOBJ:Column\A5ED3SKF.BO0003LM*/
/*						watch=next_uri4;*/
						if rc4 gt 0 then do;
							rc5=metadata_getattr(next_uri4, 'Name', map_col_nm);
							rc5=metadata_getattr(next_uri4, 'Id', map_col_id);

							rc5=metadata_getnasn(next_uri4, 'Table', 1, next_uri5);
							rc5=metadata_getattr(next_uri5, 'TableName', map_tbl_nm);
							rc5=metadata_getattr(next_uri5, 'Id', map_tbl_id);

							rc5=metadata_getnasn(next_uri5, 'TablePackage', 1, next_uri6);
							rc6=metadata_getattr(next_uri6, 'Name', map_lib_nm);
							rc5=metadata_getattr(next_uri6, 'Id', map_lib_id);
						end;
						else do;
							map_col_nm='';
						end;

/*						rc4=metadata_getnasn(next_uri3, &quotedEndPoint., 1, next_uri4);*/
						if rc4 gt 0 then do;
							rc5=metadata_getnasn(next_uri3, 'SourceCode', 1, next_uri6);
							watch=next_uri6;
							rc5=metadata_getattr(next_uri6, 'StoredText', map_rules);

							col_nm2=0;
							do until (next_uri5 eq '');
								col_nm2=col_nm2+1;
								next_uri5='';
								rc5=metadata_getnasn(next_uri3, 'SubstitutionVariables', col_nm2, next_uri5);
								if next_uri5 ne '' then do;
									xxx='';
									yyy='';
									rc5=metadata_getattr(next_uri5, 'Name', xxx);
									rc5=metadata_getnasn(next_uri5, 'AssociatedObject', 1, next_uri6);
									rc5=metadata_getnasn(next_uri6, 'Table', 1, next_uri7);
									rc5=metadata_getattr(next_uri7, 'TableName', yyy);

									if xxx ne '' then map_rules=tranwrd(map_rules,strip(scan(xxx,1,'-')),strip(yyy)||'.'||strip(scan(xxx,2,'-')||' '));
								end;
							end;
						end;
						else do;
							xxx='';
							yyy='';
							map_rules='';
						end;
					end;
					output;
				end;
				xxx='';
				yyy='';
				map_lib_nm='';
				map_tbl_nm='';
				map_col_nm='';
				map_col_id='';
				map_tbl_id='';
				map_lib_id='';
				map_trans_role='';
				map_rules='';
			end;
		end;
	run;

	proc append base=stordir.&store. data=TMP&store.;
	run;

	proc datasets library=WORK;
		delete TMP&store.;
	run;
%mend getTableDet;

%macro structure();
		length
			uri $256 name $256 id $256
		;
%mend structure;

%macro structure2();
		length
			uri $256 uri3 $256
			next_uri2 $256 next_uri3 $256 next_uri4 $256 next_uri5 $256 next_uri6 $256 next_uri7 $256
			nobj 8
			n 8
			type $256
			rc 8 rc2 8 rc3 8 rc4 8 rc5 8 rc6 8
			col_cnt 8 col_nm2 8
			inp_col_nm $256 inp_col_id $256
			map_col_nm $256 map_col_id $256 map_lib_id $256
			map_tbl_nm $256 map_lib_nm $256 map_trans_role $256 map_tbl_id $256
			map_rules $256
			watch $256
			id $256
			inp_tablenm $256
			xxx $256 yyy $256
			tbl_mode $256
		;
		drop
			uri uri3 next_uri2 next_uri3 next_uri4 next_uri5 next_uri6 next_uri7
			rc rc2 rc3 rc4 rc5 rc6
			col_cnt col_nm2
			nobj n
			xxx yyy
		;
%mend structure2;

%macro getnasl(store, type, id);
	%let quotedID=%sysfunc(quote(&id., %str(%')));

	data &store.;
		length metaasn $256;
		i=0;
		do until(rc>0);
			i+1;
			rc=metadata_getnasl("omsobj:&type.?@Id=&quotedID.",i,metaasn);
			if rc>0 then output;
		end;
	run;
%mend getnasl;

%macro iter(source, runmacro, store, key, value, structure, findKey=Columns, mode=CREATE, featureMap=.,destination=.);
	%if &mode.=CREATE %then %do;
		data stordir.&store.;
			%&structure.
			delete;
		run;
	%end;

	proc sql;
		select count(1) into :srcCnt from &source.;
	quit;

	proc sql;
		select type into :typ1-:typ%trim(&srcCnt.) from &source.;
	quit;

	%do i=1 %to &srcCnt.;
		%if &runmacro.=getTableDet %then %do;
			%&runmacro.(&store., &&typ&i.., &key., &value., &structure., &featureMap., &destination.);
		%end;
		%else %if &runmacro.=getFindKey %then %do;
			%&runmacro.(&store., &&typ&i.., &key., &value., &structure., &findkey.);
		%end;
		%else %if &runmacro.=getnasl %then %do;
			%&runmacro.(&store., &&typ&i.., &key.);
		%end;
	%end;
%mend iter;

%macro getTables(objStore, store, objtyp, featureMap, destination);
	proc sql;
		select count(1) into :objCnt from stordir.&objStore.;
	quit;

	proc sql;
		select id into :id1-:id%trim(&objCnt.) from stordir.&objStore.;
	quit;

	%let mode=CREATE;
	%do cnt=1 %to &objCnt.;
		%iter(&objtyp., getTableDet, &store., id, &&id&cnt.., structure2, mode=&mode.,
			featureMap=&featureMap., destination=&destination.);
		%let mode=APPEND;
	%end;
%mend getTables;

data objtyp;
	infile datalines delimiter=',';
	length type $256;
	input type;
	datalines;
DataTable
;
run;

%macro mapFlow(job, steplist, input, output, mapT, mapS);
	proc sql;
		create table stordir.mapflow as
			select
				distinct
				j.id as jobid,
				s.id as stepid,
				i.library_id as src_lib_id,
				i.id as src_tbl_id,
				ms.inp_col_id as src_col_id,
				o.library_id as tar_lib_id,
				o.id as tar_tbl_id,
				mt.inp_col_id as tar_col_id,
				COALESCE(s.seq,0) as step_seq,
				j.name as job_nm,
				s.name as step_nm,
				i.library_name as src_lib_nm,
				i.name as src_tbl_nm,
				ms.map_col_nm as src_col_nm,
				ms.map_rules as src_map_rules,
				o.library_name as tar_lib_nm,
				o.name as tar_tbl_nm,
				mt.inp_col_nm as tar_col_nm,
				mt.map_rules as tar_map_rules
			from stordir.&job. as j
			left join stordir.&steplist. as s on
				j.id=s.parent_id
			left join stordir.&input. as i on
				s.id=i.parent_id
			left join stordir.&output. as o on
				s.id=o.parent_id
			left join stordir.&mapT. as mt on
				o.id=mt.id
			left join stordir.&mapS as ms on
				i.id=ms.id and
				mt.map_col_id=ms.inp_col_id
			order by j.id, s.seq, i.id, o.id, mt.map_col_nm, ms.map_col_nm
		;
	quit;
%mend mapFlow;

data stordir.LogJobId;
	infile datalines delimiter=',';
	length id $256;
	input id;
	datalines;
A5ED3SKF.BW000005
;
run;

/************************************************
*************************************************
START: Please select appropriate macros to run
*************************************************
*************************************************/

/*************************************************
Various examples to run:
%macro getTableDet(store, objType, key, id, structure, featureMap, endPoint);
%getMetaObj(Job, JobList, id=., mode=CREATE);
%getMetaObj(Job, JobList, id=A59RYTQX.BY0000VQ, mode=CREATE);
%getMetaObj(TransformationStep, StepList, id=., mode=CREATE);
*************************************************/

/*************************************************
To feed selected jobs:
*************************************************/
/*%getMetaObjs(LogJobId, JobList, Job);*/

/*************************************************
To get all job
%getMetaObj(Job, JobList, id=., mode=CREATE);
*************************************************/
%getMetaObj(Job, JobList, id=., mode=CREATE);
%getMetaLinks(JobList, JobStepList, CustomAssociations, AssociatedObjects);
%getMetaObjs(JobStepList, StepList, TransformationStep);
%getMetaLinks(StepList, StepCTList, Transformations, ClassifierTargets);
%getMetaLinks(StepList, StepCSList, Transformations, ClassifierSources);
/*%getTables(StepCSList, MappingS, objtyp, SourceFeatureMaps, TransformationTargets);*/
/*%getTables(StepCTList, MappingT, objtyp, TargetFeatureMaps, TransformationTargets);*/
%getTables(StepCSList, MappingS, objtyp, SourceFeatureMaps, TargetTransformations);
%getTables(StepCTList, MappingT, objtyp, TargetFeatureMaps, TargetTransformations);
%mapFlow(JobList, JobStepList, StepCSList, StepCTList, MappingT, MappingS);

/************************************************
*************************************************
END: Please select appropriate macros to run
*************************************************
*************************************************/

/***************************************************************************************
** Exmple of troubleshooting the code
***************************************************************************************/

/*OMSOBJ:Column\A5ED3SKF.BO0003LM*/
/*OMSOBJ:FeatureMap\A5ED3SKF.C500002X*/
/*OMSOBJ:TextStore\A5ED3SKF.AG00022M*/
/*OMSOBJ:Column\A5ED3SKF.BO0001G5*/
/*FeatureSources	OMSOBJ:Column\A5ED3SKF.BO0001G5*/
/*FeatureTargets	OMSOBJ:Column\A5ED3SKF.BO0003LM*/
/*SourceCode	OMSOBJ:TextStore\A5ED3SKF.AG00022M*/
/*SubstitutionVariables	OMSOBJ:Variable\A5ED3SKF.C7000001*/

/******************************************************
* Get the Attributes
******************************************************/
%macro uriGetNATR(store, URI);
	data &store.;
		length attr metanvalue $256;
		i=0;
		do until (rc<0);
			i+1;
			rc=METADATA_GETNATR(&URI., i, attr, metanvalue);
			if rc>0 then output;
		end;
	run;
%mend uriGetNATR;

/******************************************************
* Get the Associated Links
******************************************************/
%macro uriGetNASL(store, URI);
	data &store.;
		length metaasn $256;
		i=0;
		do until (rc<0);
			i+1;
			rc=METADATA_GETNASL(&URI., i, metaasn);
			if rc>0 then output;
		end;
		drop i rc;
	run;
%mend uriGetNASL;

/******************************************************
* Get the Associated Links from given Associated Links
******************************************************/
%macro uriGetNASN(store, URI, methodList);
	data &store.;
		set &methodList.;
		length next_uri metaasn2 $256;
		i=0;
		rc=METADATA_GETNASN(&URI., %quote(metaasn), 1, next_uri);
		rc2=METADATA_GETNASL(next_uri, i, metaasn2);
		rc2=0;
		do until (rc2<0);
			i+1;
			rc2=METADATA_GETNASL(next_uri, i, metaasn2);
			if rc2>0 then output;
		end;
	run;
%mend uriGetNASN;


/*%let num=1;*/
/*%uriGetNATR(tmp&num.NATR, "OMSOBJ:Column\A5ED3SKF.BO0003LM");*/
/*%uriGetNASL(tmp&num.NSAL, "OMSOBJ:Column\A5ED3SKF.BO0003LM");*/
/*%uriGetNASN(tmp&num.NSAN, "OMSOBJ:Column\A5ED3SKF.BO0003LM", tmp&num.NSAL);*/
/**/
/*%let num=2;*/
/*/*TargetFeatureMaps	OMSOBJ:FeatureMap\A5ED3SKF.C500002X*/*/
/*%uriGetNATR(tmp&num.NATR, "OMSOBJ:FeatureMap\A5ED3SKF.C500002X");*/
/*%uriGetNASL(tmp&num.NSAL, "OMSOBJ:FeatureMap\A5ED3SKF.C500002X");*/
/*%uriGetNASN(tmp&num.NSAN, "OMSOBJ:FeatureMap\A5ED3SKF.C500002X", tmp&num.NSAL);*/
/**/
/*%let num=3;*/
/*/*FeatureSources	OMSOBJ:Column\A5ED3SKF.BO0001G5*/*/
/*%uriGetNATR(tmp&num.NATR, "OMSOBJ:Column\A5ED3SKF.BO0001G5");*/
/*%uriGetNASL(tmp&num.NSAL, "OMSOBJ:Column\A5ED3SKF.BO0001G5");*/
/*%uriGetNASN(tmp&num.NSAN, "OMSOBJ:Column\A5ED3SKF.BO0001G5", tmp&num.NSAL);*/
/**/
/*%let num=4;*/
/*/*FeatureTargets	OMSOBJ:Column\A5ED3SKF.BO0003LM*/*/
/*%uriGetNATR(tmp&num.NATR, "OMSOBJ:Column\A5ED3SKF.BO0003LM");*/
/*%uriGetNASL(tmp&num.NSAL, "OMSOBJ:Column\A5ED3SKF.BO0003LM");*/
/*%uriGetNASN(tmp&num.NSAN, "OMSOBJ:Column\A5ED3SKF.BO0003LM", tmp&num.NSAL);*/
/**/
/*%let num=5;*/
/*/*SourceCode	OMSOBJ:TextStore\A5ED3SKF.AG00022M*/*/
/*%uriGetNATR(tmp&num.NATR, "OMSOBJ:TextStore\A5ED3SKF.AG00022M");*/
/*%uriGetNASL(tmp&num.NSAL, "OMSOBJ:TextStore\A5ED3SKF.AG00022M");*/
/*%uriGetNASN(tmp&num.NSAN, "OMSOBJ:TextStore\A5ED3SKF.AG00022M", tmp&num.NSAL);*/
/**/
/*%let num=5;*/
/*/*SubstitutionVariables	OMSOBJ:Variable\A5ED3SKF.C7000001*/*/
/*%uriGetNATR(tmp&num.NATR, "OMSOBJ:Variable\A5ED3SKF.C7000001");*/
/*%uriGetNASL(tmp&num.NSAL, "OMSOBJ:Variable\A5ED3SKF.C7000001");*/
/*%uriGetNASN(tmp&num.NSAN, "OMSOBJ:Variable\A5ED3SKF.C7000001", tmp&num.NSAL);*/
