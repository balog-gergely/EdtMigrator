using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

/// <summary>
/// This program is distributed under the Microsoft Public License (MS-PL) under the copyright of Ultimate Software b.v., The Netherlands
/// By using this program or its source code you accept the Microsoft Public License.
/// </summary>
namespace EdtMigrator
{
    /// <summary>
    /// The program migrates all EDT relations into Table references on the EDTs, then creates table relations between the affected tables. 
    /// Limitations
    ///     Doesnt handle edt relations with multiple fields, but saves them to a text file as TODO list
    ///     Ignores fixed field relations 
    ///     Doesn't fix overlayered tables, only extensions and tables. New fields should be in table extensions, not in overlayerings, 
    ///         and the LCS code migration tool moves overlayered fields into table extensions.
    ///     Doesn't check overlayered EDTs
    ///     Skips all EDTs that have Array elements defined on them
    /// </summary>
    class Program
    {
        /// <summary>
        /// The path of your AOS working folder
        /// </summary>
        public const string MetadataFolder = @"C:\AOSService\PackagesLocalDirectory\";

        /// <summary>
        /// The list of models that you want the tool to run against. 
        /// The names are interpreted as subfolders of the MetadataFolder
        /// You have to include every model where the EDTs are found and also every model where they are used.
        /// </summary>
        public static List<String> TargetModels = new List<string>()
        {
            @"ApplicationSuite\ApplicationSuite.Contoso",
            @"Directory\Directory.Contoso",
        };

        /// <summary>
        /// Models that contain edts we want to read, but never alter.
        /// This is list is used when searching for parents of inherited EDTs
        /// </summary>
        public static List<String> ReadOnlyModels = new List<string>()
        {
            @"ApplicationFoundation\ApplicationFoundation",
            @"ApplicationPlatform\ApplicationPlatform",
            @"ApplicationSuite\Foundation",
            @"Calendar\Calendar",
            @"Calendar\Calendar",
            @"GeneralLedger\GeneralLedger",
            @"Personnel\Personnel",
            @"PersonnelCore\PersonnelCore",
            @"Directory\Directory",
            @"Project\Project",
            @"Ledger\Ledger",
            @"Currency\Currency",
            @"ContactPerson\ContactPerson",
            @"UnitOfMeasure\UnitOfMeasure",
            @"PersonnelManagement\PersonnelManagement",
        };


        public static List<String> ParentProblems = new List<string>();
        public static List<String> EdtsWithMultiRelationParents = new List<string>();
        public static List<String> TableReferenceWithoutRelatedNodes = new List<string>();
        public static List<String> PrimaryIdDetected = new List<string>();
        public static List<String> ForeignKeyCreated = new List<string>();
        public static List<String> ExistingForeignKeyDetected = new List<string>();
        public static List<String> AnomalyDetected = new List<string>();
        public static List<String> MultiKeyFkDetected = new List<string>();
        public static List<String> MultiRelationEdts = new List<string>();

        /// <summary>
        /// Nasty expensive structure to keep track of all EDT documents we have opened so far. Trades memory for repeated DOM parsing.
        /// </summary>
        public static Dictionary<String, XmlDocument> AllEdts = new Dictionary<string, XmlDocument>();

        /// <summary>
        /// Saves the relevant metadata about processed EDTs
        /// </summary>
        public static Dictionary<String, EdtData> AllParsedEdts = new Dictionary<string, EdtData>();

        /// <summary>
        /// Parent EDTs and the lists of the child EDTs waiting for them
        /// When we process an EDT that has a parent, but we haven't encountered that parent yet, we make a note here
        /// </summary>
        public static Dictionary<String, List<String>> ParentWatchList = new Dictionary<string, List<string>>();

        /// <summary>
        /// Edts that will be checked on the tables. If we encounter any of these EDTs, a new table relation will be created on the table (unless it already exist)
        /// </summary>
        public static Dictionary<String, EdtData> toProcess = new Dictionary<string, EdtData>();

        /// <summary>
        /// All output is written to this folder, in the same structure as it is in the origin point. 
        /// </summary>
        public static DirectoryInfo outputRootDirectory;

        static void Main(string[] args)
        {
            string currentRunTime = DateTime.Now.ToString("yyyy.MM.dd.HH.mm");
            outputRootDirectory = Directory.CreateDirectory($".\\{currentRunTime}");

            //Xml writer that produces similar formatting to the xml files generated by the Visual studio plugin
            XmlWriterSettings xwsSettings = new XmlWriterSettings();
            xwsSettings.Indent = true;
            xwsSettings.ConformanceLevel = ConformanceLevel.Document;

            //Fixing the EDTs
            foreach (string targetModel in TargetModels)
            {
                string modelFolder = Path.Combine(MetadataFolder, targetModel);
                DirectoryInfo resultFolder = Directory.CreateDirectory(ModelFolderName(outputRootDirectory, targetModel));

                //Not all target models have EDTs, some only have tables using our edts
                string edtInputFolder = Path.Combine(modelFolder, "AxEdt");
                if (!Directory.Exists(edtInputFolder))
                    continue;

                //Subfolder that contains the output edt files 
                string edtOutputFolder = Path.Combine(resultFolder.FullName, "AxEdt");
                DirectoryInfo edtFolder = Directory.CreateDirectory(edtOutputFolder);

                //Iterating through all input edts 
                foreach (string edtPath in Directory.EnumerateFiles(edtInputFolder, "*.xml"))
                {
                    //Loading the edt 
                    XmlDocument document = new XmlDocument();
                    document.Load(edtPath);
                    EdtData edt = new EdtData(document);
                    var relationNode = document.SelectSingleNode("//Relations");
                    var numberOfArrays = document.SelectNodes("//AxEdtArrayElement").Count;
                    var referenceTableNode = document.SelectSingleNode("//ReferenceTable");

                    //Checking the watchlist. It is possible that some EDTs will be parsed twice
                    CheckWatchList(document);

                    //Has an array element. Making a note, but not processing it
                    if (numberOfArrays > 0)
                    {
                        AnomalyDetected.Add($"{edt.EdtName} has Array Elements. Needs manual processing");
                        continue;
                    }

                    //This table edt has nothing interesting in it, it doesnt have a table relation, it doesnt have a table reference, and it is not inherited
                    if (relationNode == null && (referenceTableNode == null || String.IsNullOrEmpty(referenceTableNode.InnerText)) && !String.IsNullOrEmpty(edt.NameOfParentEdt))
                    {
                        continue;
                    }

                    //Saving the parsed XML
                    AllEdts.Add(edt.EdtName.ToLower(), document);

                    //There is a relation
                    if (relationNode != null && relationNode.ChildNodes.Count != 0)
                    {
                        //This edt has multiple relations in it (probably a field fixed). These will have to be manually fixed after the migration
                        if (relationNode.ChildNodes.Count > 1)
                        {
                            MultiRelationEdts.Add(edt.EdtName);
                        }

                        //Some files have to be skipped, even if they have relations
                        edt.Relations = ReadRelations(relationNode);

                        //There are some weirdly formatted EDT xmls out there
                        if (edt.Relations.Count > 0)
                        {
                            //Fix the EDT
                            ConvertToReferenceTableNode(edt, document);
                            ConvertToTableReferencesNode(edt, document);
                            relationNode.ParentNode.RemoveChild(relationNode);

                            //Save the fixed EDT
                            toProcess.Add(edt.EdtName.ToLower(), edt);
                            using (XmlWriter xwWriter = XmlWriter.Create(Path.Combine(edtFolder.FullName, new FileInfo(edtPath).Name), xwsSettings))
                                document.Save(xwWriter);
                        }

                        AllParsedEdts.Add(edt.EdtName.ToLower(), edt);
                        AllEdts.Remove(edt.EdtName.ToLower());
                    }
                    //Gathering the info about the reference tables so we can use them if there are any EDTS inherited from them
                    else if (referenceTableNode != null && !String.IsNullOrEmpty(referenceTableNode.InnerText))
                    {
                        EdtData referenceTable = ReadReferenceTable(document);
                        if (referenceTable != null)
                        {
                            toProcess.Add(referenceTable.EdtName.ToLower(), referenceTable);
                            AllParsedEdts.Add(referenceTable.EdtName.ToLower(), referenceTable);
                            AllEdts.Remove(referenceTable.EdtName.ToLower());
                        }
                    }
                    //This EDT extends another (its else if, because if the relations are defined on this edt , then we don't care about its ancestors)
                    else if (!String.IsNullOrEmpty(edt.NameOfParentEdt))
                    {
                        //We check if we already know about the edt that is the parent of this one
                        Relation relationOfParent = FindRelationForParent(edt.NameOfParentEdt, edt.EdtName);

                        //If we were able to found the parent relation, then this EDT can also be processed. 
                        //Otherwise we do nothing with it- either it's on the watchlist, waiting for a parent to turn up, 
                        //or the parent(s) don't have relations either
                        if (relationOfParent != null)
                        {
                            edt.Relations.Add(relationOfParent);
                            toProcess.Add(edt.EdtName.ToLower(), edt);
                            AllParsedEdts.Add(edt.EdtName.ToLower(), edt);
                            AllEdts.Remove(edt.EdtName.ToLower());
                        }
                    }
                }
            }

            //Reading possible parent edts
            foreach (string targetModel in ReadOnlyModels)
            {
                string modelFolder = Path.Combine(MetadataFolder, targetModel);
                string edtInputFolder = Path.Combine(modelFolder, "AxEdt");
                if (!Directory.Exists(edtInputFolder))
                    continue;

                //Iterating through all input edts 
                foreach (string edtPath in Directory.EnumerateFiles(edtInputFolder, "*.xml"))
                {
                    //Loading the edt 
                    XmlDocument document = new XmlDocument();
                    document.Load(edtPath);
                    CheckWatchList(document);
                }
            }

            //We dont work with multiRelation edts, they have to be handled manually (they should be filtered earlier, except for some really weird cases)
            foreach (string key in toProcess.Keys)
            {
                if (toProcess[key].Relations.Count > 1)
                {
                    AnomalyDetected.Add($"{key} has anomalous multiple relations");
                    toProcess.Remove(key);
                }
            }

            //Creating the table relations on the tables and table extensions
            foreach (string folder in TargetModels)
            {
                DirectoryInfo resultFolder = new DirectoryInfo(ModelFolderName(outputRootDirectory, folder));
                string modelFolder = Path.Combine(MetadataFolder, folder);

                //Checking table extensions
                if (Directory.Exists(Path.Combine(modelFolder, "AxTableExtension")))
                {
                    DirectoryInfo axTableExtensionFolder = Directory.CreateDirectory(Path.Combine(resultFolder.FullName, "AxTableExtension"));
                    foreach (string tablePath in Directory.EnumerateFiles(Path.Combine(modelFolder, "AxTableExtension"), "*.xml"))
                    {
                        XmlDocument documentToSave = UpdateTableWithRelations(tablePath);

                        //Saving the file. null is magic value for not having any updated relations
                        if (documentToSave != null)
                        {
                            using (XmlWriter xwWriter = XmlWriter.Create(Path.Combine(axTableExtensionFolder.FullName, new FileInfo(tablePath).Name), xwsSettings))
                                documentToSave.Save(xwWriter);
                        }
                    }
                }

                //Checking tables
                if (Directory.Exists(Path.Combine(modelFolder, "AxTable")))
                {
                    DirectoryInfo axTableFolder = Directory.CreateDirectory(Path.Combine(resultFolder.FullName, "AxTable"));
                    foreach (string tablePath in Directory.EnumerateFiles(Path.Combine(modelFolder, "AxTable"), "*.xml"))
                    {
                        XmlDocument documentToSave = UpdateTableWithRelations(tablePath);

                        //Saving the file. null is magic value for not having any updated relations 
                        if (documentToSave != null)
                        {
                            using (XmlWriter xwWriter = XmlWriter.Create(Path.Combine(axTableFolder.FullName, new FileInfo(tablePath).Name), xwsSettings))
                                documentToSave.Save(xwWriter);
                        }
                    }
                }
            }

            //Writing the logfiles
            foreach (var key in ParentWatchList.Keys)
            {
                ParentProblems.Add($"Parent {key} not found. Children are: {String.Join(",", ParentWatchList[key])}");
            }

            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"MultiRelationEdt.txt"), MultiRelationEdts);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"PrimaryIdDetected.txt"), PrimaryIdDetected);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"ExistingForeignKeyDetected.txt"), ExistingForeignKeyDetected);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"AnomalyDetected.txt"), AnomalyDetected);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"MultiKeyFkDetected.txt"), MultiKeyFkDetected);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"ForeignKeyCreated.txt"), ForeignKeyCreated);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"EdtsWithMultiRelationParents.txt"), EdtsWithMultiRelationParents);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"ParentProblems.txt"), ParentProblems);
            File.WriteAllLines(Path.Combine(outputRootDirectory.FullName, $"TableReferenceWithoutRelatedNodes.txt"), TableReferenceWithoutRelatedNodes);
        }

        /// <summary>
        /// Checks if we have been waiting for this edt to turn up or not 
        /// </summary>
        /// <param name="document"></param>
        private static void CheckWatchList(XmlDocument document)
        {
            EdtData parent = new EdtData(document);
            if (ParentWatchList.ContainsKey(parent.EdtName.ToLower()))
            {
                List<String> children = ParentWatchList[parent.EdtName.ToLower()];
                var relationNode = document.SelectSingleNode("//Relations");

                //Reading the relevant parts from the parent EDT
                if (document.SelectSingleNode("//ReferenceTable") != null)
                {
                    EdtData parentFromReferenceTable = ReadReferenceTable(document);
                    if (parentFromReferenceTable != null)
                    {
                        parent = parentFromReferenceTable;
                    }
                    else
                    {
                        ParentProblems.Add($"{parent.EdtName} has a Reference table node, but doesnt have valid table references, children {String.Join(",", children)} could not be processed");
                        return;
                    }
                }
                else if (relationNode != null)
                {
                    parent.Relations = ReadRelations(relationNode);
                }
                else if (!String.IsNullOrEmpty(parent.NameOfParentEdt))
                {
                    //This EDT has no valid relations, but does have a parent, so we recursively extend the search upward 
                    //in the inheritance hierarchy
                    foreach (string child in children)
                    {
                        FindRelationForParent(parent.NameOfParentEdt, child);
                    }
                    ParentWatchList.Remove(parent.EdtName.ToLower());
                }

                //We found one relation on the parent, so all children should get that relation from the parent
                if (parent.Relations.Count == 1)
                {
                    foreach (string child in children)
                    {
                        XmlDocument childXml = AllEdts[child.ToLower()];
                        EdtData childEdt = new EdtData(childXml);
                        childEdt.Relations.Add(parent.Relations[0]);
                        toProcess.Add(childEdt.EdtName.ToLower(), childEdt);
                        if (!AllParsedEdts.ContainsKey(childEdt.EdtName.ToLower()))
                            AllParsedEdts.Add(childEdt.EdtName.ToLower(), childEdt);

                        if (AllEdts.ContainsKey(childEdt.EdtName.ToLower()))
                            AllEdts.Remove(childEdt.EdtName.ToLower());
                    }
                    ParentWatchList.Remove(parent.EdtName.ToLower());
                }
                else if (parent.Relations.Count == 0)
                {
                    //No relations and no parents, the children edt don't need to be processed in the tables
                    ParentWatchList.Remove(parent.EdtName.ToLower());
                }
                else
                {
                    ParentProblems.Add($"{parent.EdtName} has multiple relations. Validate manually. The child edts are:{String.Join(",", children)}");
                }
            }
        }

        /// <summary>
        /// Parses the AX2009-style edt relations
        /// </summary>
        /// <param name="relationNode"></param>
        /// <returns></returns>
        private static List<Relation> ReadRelations(XmlNode relationNode)
        {
            List<Relation> relations = new List<Relation>();
            //Saving all the relations in the edt,but we will only process them 
            foreach (XmlNode node in relationNode.ChildNodes)
            {
                bool hasFields = false;
                Relation currentRelation = new Relation();
                foreach (XmlNode property in node.ChildNodes)
                {
                    //TODO add FixedField handling
                    switch (property.Name)
                    {
                        case "RelatedField":
                            currentRelation.RelatedField = property.InnerText;
                            hasFields = true;
                            break;
                        case "Table":
                            currentRelation.Table = property.InnerText;
                            hasFields = true;
                            break;
                        case "Value":
                            //If the Value is 0, then the Value node will completely be missing. 
                            currentRelation.Value = property.InnerText;
                            //Just having a value doesnt qualify as a "valid" edt
                            //hasFields = true;
                            break;
                        default:
                            throw new ApplicationException("Unkown property in relation");
                    }
                }

                if (hasFields)
                    relations.Add(currentRelation);
            }

            if (relations.Count > 0 && !relations.Select(a => a.Table).All(a => a == relations[0].Table))
            {
                throw new ApplicationException("Tables of edt relations dont point to the same target table");
            }

            return relations;
        }

        /// <summary>
        /// Find if the parent has a relation somewhere up on the inheritance chain
        /// </summary>
        /// <returns></returns>
        private static Relation FindRelationForParent(string parentName, string originalEdtName)
        {
            //Check if we already processed the parent into an edtData
            //If not, check if we at least parsed the XML document
            //If not, then put the parent on the waiting list, and when we come across it, we will resume processing the chain
            //And do the whole thing recursively, until we either run into a relation or run out of parents. 
            EdtData parent = null;

            //Capitalization in AX XMLs is inconsistent, so we have to go with tolower (or compare invariantly)
            string lcParentName = parentName.ToLower();
            if (AllParsedEdts.ContainsKey(lcParentName))
            {
                parent = AllParsedEdts[lcParentName];
            }
            else if (AllEdts.ContainsKey(lcParentName))
            {
                //We've read this file previously, but it didnt have table references or relations on the first pass
                //However we now know that we should save it for the future
                XmlDocument document = AllEdts[lcParentName];
                parent = new EdtData(document);
                AllParsedEdts.Add(lcParentName, parent);
            }
            else
            {
                //Parent not found yet. Adding it to watchlist
                if (ParentWatchList.ContainsKey(lcParentName))
                {
                    ParentWatchList[lcParentName].Add(originalEdtName.ToLower());
                }
                else
                {
                    ParentWatchList.Add(lcParentName, new List<string> { originalEdtName.ToLower() });
                }

                //Parent not found for now, processing will resume later through the watchlist
                return null;
            }

            if (parent.Relations.Count == 1)
            {
                //The parent has one relation, this is what we want
                return parent.Relations[0];
            }
            else if (parent.Relations.Count > 1)
            {
                EdtsWithMultiRelationParents.Add($"{originalEdtName} has an ancestor ({parentName}) with multiple relations");
                //We do nothing with this edt
                return null;
            }
            //this means the parent has 0 relations
            else
            {
                //If this parent also has a parent, we continue crawling up the inheritance chain
                if (!String.IsNullOrEmpty(parent.NameOfParentEdt))
                {
                    return FindRelationForParent(parent.NameOfParentEdt, originalEdtName);
                }
                else
                {
                    //We reached the top of the tree, it doesnt have a relation, we don't have to process this edt
                    return null;
                }
            }
        }



        /// <summary>
        /// Reads an existing (or previously migrated) table relation on an Edt 
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static EdtData ReadReferenceTable(XmlDocument document)
        {
            var referenceTableNode = document.SelectSingleNode("//ReferenceTable");
            EdtData returnValue = new EdtData(document);

            //There should be only one table reference node 
            int tableReferencesNodeCount = document.SelectNodes("//AxEdtTableReference").Count;
            if (tableReferencesNodeCount > 1)
                throw new ApplicationException("There should be only one table reference in an edt relation");
            else if (tableReferencesNodeCount == 0)
            {
                TableReferenceWithoutRelatedNodes.Add($"There is a ReferenceTableName but no AxEdtTableReference in {returnValue.EdtName}");
                return null;
            }

            //There is indeed only one
            var tableReferencesNode = document.SelectSingleNode("//AxEdtTableReference");
            var relatedFieldNode = tableReferencesNode.SelectSingleNode("./RelatedField");
            var tableNode = tableReferencesNode.SelectSingleNode("./Table");
            if (relatedFieldNode == null)
                throw new ApplicationException("There should be a related field node in an edt relation");
            if (tableNode == null)
                throw new ApplicationException("There should be a table node in an edt relation");
            string relatedField = relatedFieldNode.InnerText;
            string table = tableNode.InnerText;
            returnValue.Relations.Add(new Relation { Table = table, RelatedField = relatedField });
            return returnValue;
        }

        /// <summary>
        /// Create the relations on the table xml 
        /// </summary>
        private static XmlDocument UpdateTableWithRelations(string tablePath)
        {
            XmlDocument document = new XmlDocument();
            document.Load(tablePath);
            //Gets set if anything is changed
            bool tableIsUpdated = false;

            //Name node of the root elmenet, which can be AxTable or AxTableExtension
            String tableName = document.SelectSingleNode("/*/Name").InnerText;
            foreach (XmlNode field in document.SelectNodes("//AxTableField"))
            {
                XmlNode edtNode = field.SelectSingleNode(".//ExtendedDataType");

                if (edtNode != null && toProcess.ContainsKey(edtNode.InnerText.ToLower()))
                {
                    EdtData edt = toProcess[edtNode.InnerText.ToLower()];
                    // Found a match. However: 
                    // The field that the edt references should not get a table relation. 
                    // How can we detect this? The name of our current table must be the 
                    // same as the name of the relation and the name of the field must be the same as the name of the referenced field
                    // Every other occurance of the EDT should get a foreign key relation
                    string fieldName = field.SelectSingleNode(".//Name").InnerText;
                    string foreignTableName = edt.Relations[0].Table;
                    string foreignFieldName = edt.Relations[0].RelatedField;
                    string xPath = $".//AxTableRelation[RelatedTable=\"{foreignTableName}\" and Constraints/AxTableRelationConstraint/Field=\"{fieldName}\"]";
                    if (tableName.ToLower() == foreignTableName.ToLower() && fieldName.ToLower() == foreignFieldName.ToLower())
                    {
                        PrimaryIdDetected.Add($"{tableName} has field {fieldName} with datatype {edt.EdtName}. This is the primary key of the EDT, no relation is created");
                    }
                    else
                    {
                        //Find relation node
                        XmlNode relationNode = document.SelectSingleNode("//Relations");
                        //Not a key field, we can actually do something with it
                        if (document.SelectNodes($"//AxTableField[ExtendedDataType = \"{edt.EdtName}\"]").Count == 1)
                        {
                            //Simple case, only one copy of they key field is found

                            //Checking if FK already exists
                            //  -Right table is referenced
                            //  -On the same field
                            int existingFkCount = relationNode.SelectNodes(xPath).Count;

                            if (existingFkCount == 1)
                            {
                                XmlNode fkCandidate = relationNode.SelectSingleNode(xPath);
                                var fieldsInFk = fkCandidate.SelectNodes($".//Field");
                                if (fieldsInFk.Count == 1)
                                {
                                    var fieldInFk = fieldsInFk[0];
                                    //Checking if the related field in the other table is the expected field
                                    if (fieldInFk.ParentNode.SelectSingleNode(".//RelatedField").InnerText.ToLower() == foreignFieldName.ToLower())
                                        ExistingForeignKeyDetected.Add($"{tableName} already has FK on edt {edt.EdtName} on field {fieldName}, so nothing is added");
                                    else
                                        AnomalyDetected.Add($"{tableName} already has FK on edt {edt.EdtName} on field {fieldName} but it doesnt reference the expected field in the foreign table");
                                }
                                else
                                {
                                    MultiKeyFkDetected.Add($"{tableName} has a multi-key FK ({fkCandidate.SelectSingleNode(".//Name").InnerText} along {edt.EdtName} ");
                                }
                            }
                            else if (existingFkCount > 1)
                            {
                                AnomalyDetected.Add($"{tableName} has multiple FKs along {edt.EdtName} on field {fieldName}");
                            }
                            else
                            {
                                //No FK exists, lets do our thing
                                ForeignKeyCreated.Add($"{tableName} has a single copy of edt {edt.EdtName} on field {fieldName}. Foreign key created");
                                XmlNode fk = CreateFkNode(document, foreignTableName, foreignTableName);
                                XmlNode constraintsNode = document.CreateNode(XmlNodeType.Element, "Constraints", "");
                                XmlNode axTableRelationConstraintNode = CreateConstraintNode(document, fieldName, edt.Relations[0].RelatedField);
                                fk.AppendChild(constraintsNode);
                                constraintsNode.AppendChild(axTableRelationConstraintNode);
                                relationNode.AppendChild(fk);
                                tableIsUpdated = true;
                            }
                        }
                        else
                        {
                            //Multiple copies of the field are found (one of them might be the PK that doesnt need an FK)
                            //We have to add a role to the relations in this case
                            //Is there an FK already on this field? 
                            int existingFkCount = relationNode.SelectNodes(xPath).Count;
                            if (existingFkCount == 0)
                            {
                                //This FK doesnt exist yet, we create the structure
                                XmlNode fk = CreateFkNode(document, foreignTableName + "_" + fieldName, foreignTableName);
                                XmlNode constraintsNode = document.CreateNode(XmlNodeType.Element, "Constraints", "");
                                XmlNode axTableRelationConstraintNode = CreateConstraintNode(document, fieldName, edt.Relations[0].RelatedField);
                                XmlNode roleNode = document.CreateNode(XmlNodeType.Element, "Role", "");
                                roleNode.InnerText = $"{tableName}_{fieldName}";
                                XmlNode relatedRoleNode = document.CreateNode(XmlNodeType.Element, "RelatedTableRole", "");
                                relatedRoleNode.InnerText = $"{foreignTableName}_{fieldName}";
                                fk.AppendChild(relatedRoleNode);
                                fk.AppendChild(roleNode);
                                fk.AppendChild(constraintsNode);
                                constraintsNode.AppendChild(axTableRelationConstraintNode);
                                relationNode.AppendChild(fk);
                                tableIsUpdated = true;
                                ForeignKeyCreated.Add($"{tableName} has multiple copies of {edt.EdtName}. Foreign key created for field {fieldName}");
                            }
                            else if (existingFkCount == 1)
                            {
                                //FK exists, logging it
                                ExistingForeignKeyDetected.Add($"{tableName} already has FK on edt {edt.EdtName} on field {fieldName}, so nothing is added");
                            }
                            else
                            {
                                //Multiple FK on a single field? what is going on?
                                AnomalyDetected.Add($"{tableName} has muliple FKs on a single field ({fieldName})");
                            }
                        }
                    }
                }
            }
            //Null is the magic value indicating the file doesnt need to be saved
            if (tableIsUpdated)
                return document;
            else
                return null;
        }

        /// <summary>
        /// Creates an AxTableRelationForeignKey node on a Table
        /// </summary>
        /// <returns></returns>
        private static XmlNode CreateFkNode(XmlDocument document, string fkName, string tableName)
        {
            bool nameFinal = false;
            do
            {
                //The capitalization of table names in the foreign keys are inconsistent, it's better to check with tolower.
                string xpath = $"//AxTableRelation/Name[translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz') = \"{fkName.ToLower()}\"]";
                if (document.SelectNodes(xpath).Count == 0)
                    nameFinal = true;
                else
                {
                    fkName += "_1";
                }

            } while (!nameFinal);

            XmlNode fk = document.CreateNode(XmlNodeType.Element, "AxTableRelation", "");
            fk.Attributes.Append(document.CreateAttribute("xmlns"));
            fk.Attributes.Append(document.CreateAttribute("type", "http://www.w3.org/2001/XMLSchema-instance"));
            fk.Attributes[fk.Attributes.Count - 1].Value = "AxTableRelationForeignKey";
            fk.Attributes[fk.Attributes.Count - 1].Prefix = "i";
            XmlNode nameNode = document.CreateNode(XmlNodeType.Element, "Name", "");
            nameNode.InnerText = fkName;
            XmlNode relatedTableNode = document.CreateNode(XmlNodeType.Element, "RelatedTable", "");
            relatedTableNode.InnerText = tableName;
            fk.AppendChild(nameNode);
            fk.AppendChild(relatedTableNode);
            return fk;
        }

        /// <summary>
        /// Creates an AxTableRelationConstraint node that can be added to a foreign key later
        /// </summary>
        /// <returns></returns>
        private static XmlNode CreateConstraintNode(XmlDocument document, string fieldName, string relatedFieldName)
        {
            XmlNode axTableRelationConstraintNode = document.CreateNode(XmlNodeType.Element, "AxTableRelationConstraint", "");
            axTableRelationConstraintNode.Attributes.Append(document.CreateAttribute("xmlns"));
            axTableRelationConstraintNode.Attributes.Append(document.CreateAttribute("type", "http://www.w3.org/2001/XMLSchema-instance"));
            axTableRelationConstraintNode.Attributes[axTableRelationConstraintNode.Attributes.Count - 1].Prefix = "i";
            axTableRelationConstraintNode.Attributes[axTableRelationConstraintNode.Attributes.Count - 1].Value = "AxTableRelationConstraintField";
            XmlNode nameNode = document.CreateNode(XmlNodeType.Element, "Name", "");
            nameNode.InnerText = fieldName;
            XmlNode fieldNode = document.CreateNode(XmlNodeType.Element, "Field", "");
            fieldNode.InnerText = fieldName;
            XmlNode relatedFieldNode = document.CreateNode(XmlNodeType.Element, "RelatedField", "");
            relatedFieldNode.InnerText = relatedFieldName;
            axTableRelationConstraintNode.AppendChild(nameNode);
            axTableRelationConstraintNode.AppendChild(fieldNode);
            axTableRelationConstraintNode.AppendChild(relatedFieldNode);
            return axTableRelationConstraintNode;
        }


        /// <summary>
        /// Create the table references node
        /// </summary>
        private static void ConvertToTableReferencesNode(EdtData edt, XmlDocument document)
        {
            //Does it have a referencetable? if not, add it after the label
            XmlNode referenceNode = document.SelectSingleNode("//TableReferences");
            if (referenceNode != null)
            {
                if (!String.IsNullOrEmpty(referenceNode.InnerXml))
                    throw new ApplicationException("TableReferences is already filled");
            }
            else
            {
                XmlNode relationsNode = document.SelectSingleNode("//Relations");
                referenceNode = document.CreateNode(XmlNodeType.Element, "TableReferences", "");
                document.FirstChild.NextSibling.InsertAfter(referenceNode, relationsNode);
            }

            //Multi relation EDTs are not processed by the program, they are left for manual fixing
            Relation rel = edt.Relations.First(a => String.IsNullOrEmpty(a.Value));
            if (rel == null)
                throw new ApplicationException("Only fixed field relations were found on the edt");

            XmlNode edtTableReferenceNode = document.CreateNode(XmlNodeType.Element, "AxEdtTableReference", "");
            referenceNode.AppendChild(edtTableReferenceNode);

            XmlNode relatedFieldNode = document.CreateNode(XmlNodeType.Element, "RelatedField", "");
            edtTableReferenceNode.AppendChild(relatedFieldNode);
            relatedFieldNode.InnerText = rel.RelatedField;

            XmlNode relatedTableNode = document.CreateNode(XmlNodeType.Element, "Table", "");
            edtTableReferenceNode.AppendChild(relatedTableNode);
            relatedTableNode.InnerText = rel.Table;
        }

        /// <summary>
        /// Create the ReferenceTable node
        /// </summary>
        private static void ConvertToReferenceTableNode(EdtData edt, XmlDocument document)
        {
            //Does it have a referencetable? if not, add it after the label
            XmlNode referenceNode = document.SelectSingleNode("//ReferenceTable");
            if (referenceNode != null)
            {
                if (!String.IsNullOrEmpty(referenceNode.InnerText))
                    throw new ApplicationException("Reference table is already filled");
            }
            else
            {
                //Order of nodes is fixed in XML, but schema isnt available. So we take a best guess abou where to insert the ReferenceTable node
                XmlNode insertAfterNode = document.SelectSingleNode("//Label");
                if (insertAfterNode == null)
                    insertAfterNode = document.SelectSingleNode("//HelpText");
                if (insertAfterNode == null)
                    insertAfterNode = document.SelectSingleNode("//Extends");
                if (insertAfterNode == null)
                    insertAfterNode = document.SelectSingleNode("//Name");

                referenceNode = document.CreateNode(XmlNodeType.Element, "ReferenceTable", "");
                document.FirstChild.NextSibling.InsertAfter(referenceNode, insertAfterNode);
            }
            referenceNode.InnerText = edt.Relations[0].Table;
        }

        static string ModelFolderName(DirectoryInfo root, string folder)
        {
            return Path.Combine(root.FullName, folder);
        }
    }
}
